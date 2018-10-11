# from pulpcore.plugin.stages import ProfilingQueue
import asyncio


class Node:
    def __init__(self, stage, prev_stages=None, following_stages=None):
        self.stage = stage
        self.prev_stages = prev_stages or []
        self.following_stages = following_stages or []

    def full_repr(self):
        ins = "        \n".join([repr(q) for q in self.prev_stages])
        outs = "        \n".join([repr(q) for q in self.following_stages])
        stage_str = "Stage: {stage}\n".format(stage=self.stage)
        in_str = "    In:\n        {ins}\n".format(ins=ins)
        out_str = "    Out:\n        {outs}".format(outs=outs)
        return stage_str + in_str + out_str

    def __repr__(self):
        return self.stage

    def _add_prev(self, stage):
        self.prev_stages.append(stage)

    def add_following(self, stage):
        stage._add_prev(self)
        self.following_stages.append(stage)


class NamedQueue(asyncio.Queue):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return "Queue: {name}".format(name=self.name)


# n1_in = NamedQueue('n1_in')
# n1_out = NamedQueue('n1_out')
n1 = Node("N1")
n2 = Node("N2")
n1.add_following(n2)

print(n1.full_repr())
print()
print()
print(n2)

#
#
# futures = []
# if settings.PROFILE_STAGES_API:
#     in_q = ProfilingQueue.make_and_record_queue(stages[0], 0, maxsize)
#     for i, stage in enumerate(stages):
#         next_stage_num = i + 1
#         if next_stage_num == len(stages):
#             out_q = None
#         else:
#             next_stage = stages[next_stage_num]
#             out_q = ProfilingQueue.make_and_record_queue(next_stage, next_stage_num, maxsize)
#         futures.append(asyncio.ensure_future(stage(in_q, out_q)))
#         in_q = out_q
# else:
#     in_q = None
#     for stage in stages:
#         out_q = asyncio.Queue(maxsize=maxsize)
#         futures.append(asyncio.ensure_future(stage(in_q, out_q)))
#         in_q = out_q
#
#
