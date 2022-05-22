import time


def map_func(input):
    return input


def reduce_func(input):
    return input


class reducer:
    def __init__(self):
        self.state = {}
        self.processed= 0

    def execute(self, element):
        print(self.processed)
        if element in self.state:
            time.sleep(0.2)
            self.state[element] += 1
        else:
            time.sleep(0.2)
            self.state[element] = 1
        self.processed += 1

    def done(self):
        # print(self.state)
        print("PROCESSED", self.processed)
        return self.state
