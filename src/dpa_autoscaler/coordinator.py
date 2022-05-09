import ray
import time

from ray.util.queue import Queue

from dpa_autoscaler.runtime import Mapper, Reducer
from dpa_autoscaler.autoscaler import AutoScaler


@ray.remote
class MapReduceCoordinator:
    def __init__(
        self,
        input_data,
        num_mappers,
        num_reducers,
        mapper,
        reducer,
        output_queue,
        autoscale=False,
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

        self.finished = False

        self.output_queue = output_queue

        self.autoscale = autoscale

    def run(self):
        if self.autoscale:
            ray.get(AutoScaler.options(name="autoscaler").remote())

        reducers = []
        reducer_queues = []

        for i in range(self.num_reducers):
            input_queue = Queue()

            reducer_queues.append(input_queue)

            reducers.append(
                Reducer.options(max_concurrency=2).remote(
                    self.reducer,
                    f"reducer-{i}",
                    "coordinator",
                    input_queue,
                    self.output_queue,
                )
            )

        mappers = [
            Mapper.remote(self.mapper, f"mapper-{i}", "coordinator", reducer_queues)
            for i in range(self.num_mappers)
        ]

        self.start_time = time.perf_counter()

        for mapper in mappers:
            mapper.process.remote()

        for reducer in reducers:
            reducer.process.remote()

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
