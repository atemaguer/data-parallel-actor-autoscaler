import time
import ray

from ray.util.queue import Queue

ray.init(ignore_reinit_error=True)

alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'] * 1000

def map_func(input):
    return input

def reduce_func(input):
    return input

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
    def __init__(self, reducer, name, coordinator_name, input_queue, output_queue, *args):
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

@ray.remote
class MapReduceCoordinator:
    def __init__(self, input_data, num_mappers, num_reducers, mapper, reducer, output_queue):
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

        self.current_state = "not_started"

        self.output_queue = output_queue

        for i in range(self.num_reducers):
            input_queue = Queue()
            self.reducer_queues.append(input_queue)
            self.reducers.append(Reducer.remote(self.reducer, f'reducer-{i}', "coordinator", input_queue, self.output_queue))

        self.mappers = [Mapper.remote(self.mapper, f'reducer-{i}', "coordinator", self.reducer_queues) for i in range(self.num_mappers)]

    def run(self):
        self.start_time = time.perf_counter()

        for mapper in self.mappers:
            mapper.process.remote()

        self.current_state = "mapping"

    def register_mapper(self):
        self.done_mappers += 1

        if self.done_mappers == self.num_mappers:
            for reducer in self.reducers:
                reducer.process.remote()
            self.current_state = "reducing"


    def register_reducer(self):
        self.done_reducers += 1
        if self.done_reducers == self.num_reducers:
            self.running_time = (time.perf_counter() - self.start_time)

            self.current_state = "done"
            self.done_reducers = 0
            self.done_mappers = 0

    def get_mapper_input(self):
        if len(self.input_data) > 0:
            return self.input_data.pop(0)
        return None

    def get_running_time(self):
        return self.running_time

    def get_current_state(self):
        return self.current_state

@ray.remote
class AutoScaler:
    def __init__(self, *args):
        pass

    def update_state(self):
        pass

    def autoscale(self):
        pass

for (num_mappers, reducers) in [(2,4), (4,4), (4, 6), (4, 10)]:
    out_queue = Queue()
    coord = MapReduceCoordinator.options(name="coordinator").remote(alphabet, num_mappers, reducers, map_func, reduce_func, out_queue)
    coord.run.remote()

    while ray.get(coord.get_current_state.remote()) != "done":
        time.sleep(1)

    print(f'Experiment using {num_mappers} mappers and {reducers} reducers: \n')
    print(f'Running time is: {ray.get(coord.get_running_time.remote())}')

    ray.kill(coord)



