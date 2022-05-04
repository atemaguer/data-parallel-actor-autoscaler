import ray


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
            data = ray.get(coordinator.get_mapper_input.remote())

            if data:
                output = self.mapper(data)
                idx = hash(output) % len(self.reducer_queues)
                self.reducer_queues[idx].put(output)

            else:
                break
        coordinator.register_mapper.remote()
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

        while not self.input_queue.empty():
            data = self.input_queue.get()
            output = self.reducer(data)
            self.output_queue.put(output)

        self.input_queue.shutdown()

        coordinator.register_reducer.remote()
        self.done = True

    def update_auto_scaler(self):
        pass

    def done(self):
        return self.done
