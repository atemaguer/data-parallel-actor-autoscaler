import threading

import ray


class Node:
    def __init__(self):
        pass


@ray.remote
class Mapper:
    def __init__(self, mapper, name, coordinator_name, reducer_queues, *args):
        self.mapper = mapper
        self.name = name
        self.reducer_queues = reducer_queues
        self.coordinator_name = coordinator_name
        self.done = False

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)

        while True:
            data = ray.get(coordinator.mapper_input.remote())

            if data is not None:
                output = self.mapper(data)
                idx = hash(output) % len(self.reducer_queues)
                self.reducer_queues[idx].put(output)
            else:
                break

        for reducer_queue in self.reducer_queues:
            reducer_queue.put(None)

        # coordinator.register_mapper.remote()
        self.done = True

    def done(self):
        return self.done


@ray.remote
class Reducer:
    def __init__(
        self, reducer, name, coordinator_name, input_queue, output_queue, *args
    ):
        self.reducer = reducer
        self.name = name
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.coordinator_name = coordinator_name
        self.done = False

    def process(self):
        coordinator = ray.get_actor(self.coordinator_name)

        while True:
            data = self.input_queue.get()
            if data is None:
                break
            output = self.reducer(data)
            self.output_queue.put(output)

        self.input_queue.shutdown()

        coordinator.register_reducer.remote()
        self.done = True

    def update_auto_scaler_state(self):
        pass

    def done(self):
        return self.done
