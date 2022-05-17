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
            autoscaler = ray.get_actor("autoscaler")
            autoscaler.register_mapper.remote(self.name)

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)

        while True:
            data = ray.get(coordinator.mapper_input.remote())

            if data is not None:
                output = self.mapper(data)
                idx = self.ch.key_lookup(
                    output
                )  # hash(output) % len(self.reducer_queues)
                self.reducer_queues[idx].put(output)
            else:
                break

        for reducer_queue in self.reducer_queues:
            reducer_queue.put(None)

        self.done = True

    def reschedule_output(self, node_idx, num_tokens):
        self.ch.update(node_idx, num_tokens)

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
        output_queue,
        autoscale=False,
        *args,
    ):
        self.reducer = reducer
        self.name = name
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.coordinator_name = coordinator_name
        self.autoscale = autoscale
        self.done = False

        if self.autoscale:

            self.autoscaler = ray.get_actor("autoscaler")
            self.autoscaler.register_reducer.remote(self.name)

            self.autoscaler_updater = threading.Thread(
                target=self.update_auto_scaler_state, args=()
            )

            self.autoscaler_updater.start()

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)

        while True:
            data = self.input_queue.get()
            if data is None:
                break
            output = self.reducer.execute(data)
            if output is not None:
                self.output_queue.put(output)

        output = self.reducer.done()
        if output is not None:
            self.output_queue.put(output)

        self.input_queue.shutdown()

        coordinator.register_reducer.remote()
        self.done = True

    def update_auto_scaler_state(self):
        while True:
            self.autoscaler.update_reducer_state.remote(
                self.name, self.input_queue.size()
            )
            time.sleep(0.1)

    def done(self):
        return self.done
