async def create_sync_pipeline(maxsize=100):
    """
    A coroutine that builds a Stages API linear pipeline from the list `stages` and runs it.

    Each stage is a coroutine and reads from an input :class:`asyncio.Queue` and writes to an output
    :class:`asyncio.Queue`. When the stage is ready to shutdown it writes a `None` to the output
    queue. Here is an example of the simplest stage that only passes data.

    >>> async def my_stage(in_q, out_q):
    >>>     while True:
    >>>         item = await in_q.get()
    >>>         if item is None:  # Check if the previous stage is shutdown
    >>>             break
    >>>         await out_q.put(item)
    >>>     await out_q.put(None)  # this stage is shutdown so send 'None'

    Args:
        stages (list of coroutines): A list of Stages API compatible coroutines.
        maxsize (int): The maximum amount of items a queue between two stages should hold. Optional
            and defaults to 100.

    Returns:
        A single coroutine that can be used to run, wait, or cancel the entire pipeline with.
    """
    futures = []
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

    first_stage = DockerTagListStage()
    first_in = None
    tag_out = asyncio.Queue(maxsize=maxsize)
    futures.append(asyncio.ensure_future(first_stage(first_in, tag_out)))

    tag_stage = DockerTagStage()
    tag_in = tag_out
    man_out = asyncio.Queue(maxsize=maxsize)
    list_out = asyncio.Queue(maxsize=maxsize)
    processed_dcs = asyncio.Queue(maxsize=maxsize)
    # These manifests and manifest lists are already saved.
    futures.append(asyncio.ensure_future(tag_stage(tag_in, man_out, list_out, processed_dcs)))

    list_stage = DockerManifestListStage()
    list_in = list_out
    futures.append(asyncio.ensure_future(list_stage(list_in, man_out, processed_dcs)))

    man_stage = DockerManifestStage()
    man_in = man_out
    blobs_out = asyncio.Queue(maxsize=maxsize)
    futures.append(asyncio.ensure_future(man_stage(man_in, blobs_out, processed_dcs)))

    blob_stage = DockerBlobStage()
    blobs_in = blobs_out
    futures.append(asyncio.ensure_future(blob_stage(blobs_in, processed_dcs)))

    # TODO(!breaksyncstages)create "Confluence"
    # - A "confluence" is where 2 rivers join together. Here, a confluence is where 2
    #   stage streams are treated as a single asyncio.Queue object.
    # - this can be:
    #      - ConfluenceStage(in_1, in_2, out_combined)
    #           - Initial preference.
    #           - PRO: more clearly present the structure of an already complex
    #             flow by showing where the streams are joined.
    #           - CON: stage structure is more complex
    #           - Impression. The value of a very clear "circuit diagram" seems like it is worth
    #             the cost of implementation in almost every case. Perhaps it would be useful
    #             to log the stage order for all plugins, even with linear designs.

    #      - For stages that it is convinient to override run(), is probably possible to do there.
    #           - inital thinking: it would be a pain to repeat this logic everywhere, and more of
    #                              pain because implementation would necessarily have to differ for
    #                              stages that have a complicated run().
    #      - Inititial Choice: ConfluenceStage

    # TODO(breaksyncstages) Add Pulp stages











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




