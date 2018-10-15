import asyncio

from .stages import StartEmit2, EvenInOddOut, ConfluenceStage, SanityStage, DidItWorkStage


async def create_sync_pipeline(maxsize=100):
    futures = []

    even_out = asyncio.Queue()
    odd_out = asyncio.Queue()
    start = StartEmit2()
    futures.append(asyncio.ensure_future(start(even_out, odd_out)))

    even_in = even_out
    second_odd_out = asyncio.Queue()
    make_odd = EvenInOddOut()
    futures.append(asyncio.ensure_future(make_odd(even_in, second_odd_out)))

    jack_in = odd_out
    walter_in = second_odd_out
    together_out = asyncio.Queue()
    confluence = ConfluenceStage()
    futures.append(asyncio.ensure_future(confluence(jack_in, walter_in, together_out)))

    # sanity_stage = SanityStage()
    # basic_shit = asyncio.Queue()
    # futures.append(asyncio.ensure_future(sanity_stage(basic_shit)))

    # in_q = odd_out
    # in_q = basic_shit
    in_q = together_out
    omg_please = DidItWorkStage()
    futures.append(asyncio.ensure_future(omg_please(in_q)))

    try:
        await asyncio.gather(*futures)
    except Exception:
        # One of the stages raised an exception, cancel all stages...
        pending = []
        for task in futures:
            if not task.done():
                task.cancel()
                pending.append(task)
        # ...and run until all Exceptions show up
        if pending:
            await asyncio.wait(pending, timeout=60)
        raise
