import asyncio

from .stages import (SanityStage, DidItWorkStage, ConfluenceStage,
                     TagListStage)

from pulpcore.plugin.stages import(
    ArtifactDownloader,
    ArtifactSaver, QueryExistingArtifacts,
    ContentUnitAssociation, ContentUnitAssociation, ContentUnitSaver,
    QueryExistingContentUnits
)


async def create_sync_pipeline(remote, maxsize=100):
    futures = []

    inc_tag_out = asyncio.Queue(maxsize=maxsize)
    tag_list_stage = TagListStage(remote, inc_tag_out)
    futures.append(asyncio.ensure_future(tag_list_stage()))

    inc_tag_in = inc_tag_out
    tag_out = asyncio.Queue(maxsize=maxsize)
    tag_download_stage = ArtifactDownloader()
    futures.append(asyncio.ensure_future(tag_download_stage(inc_tag_in, tag_out)))

    # tag_name_in = tag_name_out
    # tag_out = asyncio.Queue()
    # inc_manifest_out = asyncio.Queue()
    # manifest_out = asyncio.Queue()
    # manifest_list_out = asyncio.Queue()
    # blob_out = asyncio.Queue()
    # tag_stage = TagDownloadStage(remote=remote)
    # futures.append(
    #     asyncio.ensure_future(
    #         tag_stage(tag_name_in, tag_out, inc_manifest_out,
    #                   manifest_out, manifest_list_out, blob_out)
    #     )
    # )
    #
    # inc_manifest_in = inc_manifest_out
    # manifest_stage = ManifestDownloadStage(remote=remote)
    # futures.append(asyncio.ensure_future(manifest_stage(inc_manifest_in, manifest_out, blob_out)))

    # # list_stage = DockerManifestListStage()
    # # list_in = list_out
    # # futures.append(asyncio.ensure_future(list_stage(list_in, man_out, processed_dcs)))

    # # man_stage = DockerManifestStage()
    # # man_in = man_out
    # # blobs_out = asyncio.Queue(maxsize=maxsize)
    # # futures.append(asyncio.ensure_future(man_stage(man_in, blobs_out, processed_dcs)))

    # # blob_stage = DockerBlobStage()
    # # blobs_in = blobs_out
    # # futures.append(asyncio.ensure_future(blob_stage(blobs_in, processed_dcs)))
    # # stages = self.pipeline_stages(new_version)

    # # stages.append(ContentUnitAssociation(new_version))
    # # if self.mirror:
    # #     stages.append(ContentUnitUnassociation(new_version))
    # # stages.append(EndStage())

    # confluence = ConfluenceStage()
    # futures.append(asyncio.ensure_future(
    #     confluence([tag_out, manifest_list_out, manifest_out, blob_out], dc_out)))

    # sanity_stage = SanityStage()
    # basic_shit = asyncio.Queue()
    # futures.append(asyncio.ensure_future(sanity_stage(basic_shit)))

    # in_q = odd_out
    # in_q = basic_shit
    # in_q = together_out
    # in_q = dc_out
    in_q = tag_out
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
