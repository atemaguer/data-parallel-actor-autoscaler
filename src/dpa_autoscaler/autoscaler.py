import ray


class AutoScalingPolicy:
    def __init__(self, *args):
        pass


@ray.remote
class AutoScaler:
    def __init__(self, *args):
        self.actor_state = {}

    def update_actor_state(self, actor_id, *args):
        pass

    def register_actor(self, actor_id, *args):
        pass

    def autoscale(self, actor_id, *args):
        pass
