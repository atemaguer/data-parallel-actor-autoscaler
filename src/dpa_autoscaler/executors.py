import time
import collections


def map_func(x):
    return x


def reduce_func(x):
    return x


class Reducer:
    def __init__(self):
        self.state = collections.defaultdict(int)

    def execute(self, element):
        time.sleep(3)
        self.state[element] += 1

    def done(self):
        # print(self.state)
        return self.state
