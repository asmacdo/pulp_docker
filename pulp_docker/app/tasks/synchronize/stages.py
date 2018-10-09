"""
The purpose of these stages is to mock out a pattern for how stages can emit to more than one
out_q, and other stages can recieve input from more than one in_q.

To start with, we need to create a stage that emits 2 outputs.
  - each q needs to be created ahead of time by the pipeline.
  - pass the qs into the stage as it's future is ensured
  - ensure_future adds the stage to the event loop
"""
import asyncio

from pulpcore.plugin.stages import Stage

import logging
log = logging.getLogger("CONFLUENCE SANITY STAGES")


class StartEmit2(Stage):
    """
    Starts without an in_q, emits to 2 different out_qs.
    """
    async def __call__(self, even_out, odd_out):
        for i in range(1, 10):
            if i % 2 == 0:
                await even_out.put(i)
            else:
                await odd_out.put(i)
            log.info("--------------------------------------------------------{i}--------------------STart".format(i=i))
        await even_out.put(None)
        await odd_out.put(None)


class EvenInOddOut(Stage):
    async def __call__(self, even_in, odd_out):
        while True:
            even = await even_in.get()
            if even is None:
                break
            # Extra digits will show which q it went through.
            new_odd = (even * 1000) + 1
            log.info("eveninoddout---------------{i}------------------".format(i=new_odd))
            await odd_out.put(new_odd)
        await odd_out.put(None)


class ConfluenceStage(Stage):
    async def __call__(self, jack_in, walter_in, together_out):
        self._pending = set()
        self._finished = set()
        jack_task = self.add_to_pending(jack_in.get())
        walter_task = self.add_to_pending(walter_in.get())
        open_jack = True
        open_walter = True
        while open_jack or open_walter:
            if self._pending:
                done, self._pending = await asyncio.wait(
                    self._pending,
                    return_when=asyncio.FIRST_COMPLETED
                )
                self._finished = self._finished.union(done)

            while self._finished:
                in_from_one_of_em = self._finished.pop()
                if in_from_one_of_em.result() is None:
                    if in_from_one_of_em is jack_task:
                        open_jack = False
                    elif in_from_one_of_em is walter_task:
                        open_walter = False
                else:
                    if in_from_one_of_em is jack_task:
                        jack_task = self.add_to_pending(jack_in.get())
                    elif in_from_one_of_em is walter_task:
                        walter_task = self.add_to_pending(walter_in.get())

                await together_out.put(in_from_one_of_em.result())
            # if (open_walter or open_jack) and not (open_walter and open_jack):
            #     import ipdb; ipdb.set_trace()

        # After both inputs are finished (2 Nones) we close this stage
        await together_out.put(None)

    def add_to_pending(self, coro):
        task = asyncio.ensure_future(coro)
        self._pending.add(asyncio.ensure_future(task))
        return task


class SanityStage(Stage):
    async def __call__(self, out_q):
        for i in range(10):
            await out_q.put(i)
        await out_q.put(None)


class DidItWorkStage(Stage):
    async def __call__(self, in_q):
        while True:
            log_it = await in_q.get()
            if log_it is not None:
                log.info(log_it)
            else:
                break
