import ray
import time

from ray.util.queue import Queue

from dpa_autoscaler.runtime import Mapper, Reducer


@ray.remote
class MapReduceCoordinator:
    def __init__(
        self, input_data, num_mappers, num_reducers, mapper, reducer, output_queue
    ):
        self.input_data = input_data

        self.start_time = 0
        self.running_time = 0

        self.mapper = mapper
        self.reducer = reducer

        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

        self.done_mappers = 0
        self.done_reducers = 0

        self.reducers = []
        self.reducer_queues = []

        self.finished = False

        self.output_queue = output_queue

        for i in range(self.num_reducers):
            input_queue = Queue()

            self.reducer_queues.append(input_queue)

            self.reducers.append(
                Reducer.remote(
                    self.reducer,
                    f"reducer-{i}",
                    "coordinator",
                    input_queue,
                    self.output_queue,
                )
            )

        self.mappers = [
            Mapper.remote(
                self.mapper, f"mapper-{i}", "coordinator", self.reducer_queues
            )
            for i in range(self.num_mappers)
        ]

    def run(self):
        self.start_time = time.perf_counter()

        for mapper in self.mappers:
            mapper.process.remote()

        for reducer in self.reducers:
            reducer.process.remote()

    def register_mapper(self):
        pass
        # self.done_mappers += 1

        # if self.done_mappers == self.num_mappers:
        #     for reducer in self.reducers:
        #         reducer.process.remote()
        # self.current_state = "reducing"

    def register_reducer(self):
        self.done_reducers += 1
        if self.done_reducers == self.num_reducers:
            self.running_time = time.perf_counter() - self.start_time

            self.finished = True
            self.done_reducers = 0

    def mapper_input(self):
        if len(self.input_data) > 0:
            return self.input_data.pop(0)
        return None

    def running_time(self):
        return self.running_time

    def is_done(self):
        return self.finished
