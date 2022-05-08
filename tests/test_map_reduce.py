import time
import ray

from ray.util.queue import Queue
from dpa_autoscaler.coordinator import MapReduceCoordinator

ray.init(address="auto", ignore_reinit_error=True)

NUM_REPEATS = 10

alphabet = [
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
] * NUM_REPEATS


def map_func(input):
    return input


class reducer():
    def __init__(self):
        self.state = {}
    def execute(self,element):
        if element in self.state:
            self.state[element] += 1
        else:
            self.state[element] = 1
    def done(self):
        print(self.state)
        return self.state


print("Running experiments:")

for (num_mappers, reducers) in [(2, 4), (4, 4), (4, 6), (4, 10)]:
    out_queue = Queue()
    reduce_func = reducer()
    coord = MapReduceCoordinator.options(name="coordinator").remote(
        alphabet, num_mappers, reducers, map_func, reduce_func, out_queue
    )

    coord.run.remote()

    while not ray.get(coord.is_done.remote()):
        time.sleep(1)

    print(f"Experiment using {num_mappers} mappers and {reducers} reducers: \n")
    print(f"Running time is: {ray.get(coord.running_time.remote())}")

    ray.kill(coord)
