import threading
import time

import ray

from dpa_autoscaler.allocation import ConsistentHashing


class Node:
    def __init__(self):
        pass


@ray.remote
class Mapper:
    def __init__(
        self, mapper, name, coordinator_name, reducer_queues, autoscale=False, *args
    ):
        self.mapper = mapper
        self.name = name
        self.reducer_queues = reducer_queues
        self.coordinator_name = coordinator_name
        self.done = False
        self.ch = ConsistentHashing(nodes=len(self.reducer_queues))
        self.autoscale = autoscale

        if self.autoscale:
            self.autoscaler = ray.get_actor("autoscaler")
            self.autoscaler.register_mapper.remote(self.name)

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)

        while True:
            data = ray.get(coordinator.mapper_input.remote())

            if data is not None:
                output = self.mapper(data)
                if self.autoscale:
                    idx = ray.get(self.autoscaler.key_lookup.remote(output))
                else:
                    idx = hash(output) % len(self.reducer_queues)
                self.reducer_queues[idx].put(output)
            else:
                break

        for reducer_queue in self.reducer_queues:
            reducer_queue.put(None)

        self.done = True

    def reschedule_output(self, node_idx):
        self.ch.halve_tokens_for_node(node_idx=node_idx)

    def done(self):
        return self.done


@ray.remote
class Reducer:
    def __init__(
        self,
        reducer,
        name,
        coordinator_name,
        input_queue,
        other_reducer_input_queues,
        output_queue,
        autoscale=False,
        *args,
    ):
        self.reducer = reducer
        self.name = name
        self.id = int(name.split("-")[1])
        self.input_queue = input_queue
        self.reducer_queues = other_reducer_input_queues
        self.output_queue = output_queue
        self.coordinator_name = coordinator_name
        self.autoscale = autoscale
        self.done = False

        if self.autoscale:
            self.autoscaler = ray.get_actor("autoscaler")
            self.autoscaler.register_reducer.remote(self.name)

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)
        counter = 0
        while True:

            data = self.input_queue.get()
            if data is None:
                ray.get(coordinator.increment_none_count.remote())
                continue
            counter += 1
            if counter % 100 == 0 and self.autoscale:
                self.update_auto_scaler_state()
            if self.autoscale:
                idx = ray.get(self.autoscaler.key_lookup.remote(data))
                if idx != self.id:
                    print("forwarding")
                    try:
                        self.reducer_queues[idx].put(data)
                    except:
                        print(data)
                        raise Exception
                    continue
                
            output = self.reducer.execute(data)
            if output is not None:
                self.output_queue.put(output)
            if ray.get(coordinator.can_die.remote()):
                print("reducer dying")
                break

                

        output = self.reducer.done()
        if output is not None:
            self.output_queue.put(output)

        #self.input_queue.shutdown()

        coordinator.register_reducer.remote()
        self.done = True

    def update_auto_scaler_state(self):
        print("updating autoscaler:", self.input_queue.size())
        self.autoscaler.update_reducer_state.remote(self.name, self.input_queue.size())

    def done(self):
        return self.done
