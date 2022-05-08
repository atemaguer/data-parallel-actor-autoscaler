def map_func(input):
    return input


def reduce_func(input):
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
