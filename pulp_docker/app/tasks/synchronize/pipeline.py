import asyncio

from .stages import (ConfluenceStage, ProcessTagStage,
                     ProcessManifestStage, DidItWorkStage,
                     QueryAndSaveArtifacts, TagListStage)

from pulpcore.plugin.stages import (
    ArtifactDownloader,
    ContentUnitAssociation, ContentUnitUnassociation, ContentUnitSaver,
    EndStage
)

# from .stages import
##############################################################################
# def pipeline_stages(self, new_version):
#     """
#     Build the list of pipeline stages feeding into the
#     ContentUnitAssociation stage.
#
#     Plugin-writers may override this method to build a custom pipeline. This
#     can be achieved by returning a list with different stages or by extending
#     the list returned by this method.
#
#     Args:
#         new_version (:class:`~pulpcore.plugin.models.RepositoryVersion`): The
#             new repository version that is going to be built.
#
#     Returns:
#         list: List of :class:`~pulpcore.plugin.stages.Stage` instances
#
#     """
#     return [
#         self.first_stage,
#         QueryExistingArtifacts(), ArtifactDownloader(), ArtifactSaver(),
#         QueryExistingContentUnits(), ContentUnitSaver(),
#     ]
#
# def create(self):
#     """
#     Perform the work. This is the long-blocking call where all syncing occurs.
#     """
#     with WorkingDirectory():
#         with RepositoryVersion.create(self.repository) as new_version:
#             loop = asyncio.get_event_loop()
#             stages = self.pipeline_stages(new_version)
#             stages.append(ContentUnitAssociation(new_version))
#             if self.mirror:
#                 stages.append(ContentUnitUnassociation(new_version))
#             stages.append(EndStage())
#             pipeline = create_pipeline(stages)
#             loop.run_until_complete(pipeline)
##############################################################################


async def create_sync_pipeline(remote, new_version, maxsize=100):
    futures = []

    pending_tag_out = asyncio.Queue(maxsize=maxsize)
    tag_list_stage = TagListStage(remote)
    futures.append(asyncio.ensure_future(tag_list_stage(pending_tag_out)))

    # Tags should always replace previous tags, dont queryexisting?
    # query_existing = QueryExistingArtifacts()

    pending_tag_in = pending_tag_out
    unprocessed_tag_out = asyncio.Queue(maxsize=maxsize)
    tag_download_stage = ArtifactDownloader()
    futures.append(asyncio.ensure_future(
        tag_download_stage(pending_tag_in, unprocessed_tag_out))
    )

    unprocessed_tag_in = unprocessed_tag_out
    processed_manifest_list_out = asyncio.Queue()
    pending_manifest_out = asyncio.Queue()
    pending_blob_out = asyncio.Queue()
    processed_tag_out = asyncio.Queue()
    process_tag_stage = ProcessTagStage(remote=remote)
    futures.append(asyncio.ensure_future(
        process_tag_stage(unprocessed_tag_in, processed_manifest_list_out,
                          pending_manifest_out, pending_blob_out1, processed_tag_out)
    ))

    in_q = processed_tag_out
    omg_please = DidItWorkStage()
    futures.append(asyncio.ensure_future(omg_please(in_q)))

    # unprocessed_manifest_list_in = unprocessed_manifest_list_out
    # processed_manifest_list_out = asyncio.Queue()
    # pending_manifest_out = asyncio.Queue()
    # process_manifest_list_stage = ProcessManifestListStage()
    # futures.append(asyncio.ensure_future(
    #     process_manifest_list_stage(unprocessed_manifest_list_in, processed_manifest_list_out,
    #                                 pending_manifest_out)
    # ))
    #
    # pending_manifest_in = pending_manifest_out
    # unprocessed_manifest_out2 = asyncio.Queue()
    # manifest_download_stage = ArtifactDownloader()
    # futures.append(asyncio.ensure_future(
    #     manifest_download_stage(pending_manifest_in, unprocessed_tag_out)
    # ))
    #
    # in_qs = [unprocessed_manifest_out1, unprocessed_manifest_out2]
    # unprocessed_manifest_out = asyncio.Queue()
    # manifest_confluence_stage = ConfluenceStage()
    # futures.append(asyncio.ensure_future(
    #     manifest_confluence_stage(in_qs, unprocessed_manifest_out)
    # ))
    #
    # unprocessed_manifest_in = unprocessed_manifest_out
    # pending_blob_out = asyncio.Queue()
    # processed_manifest_out = asyncio.Queue()
    # process_manifest_stage = ProcessManifestStage()
    # futures.append(asyncio.ensure_future(
    #     process_manifest_stage(unprocessed_manifest_in, pending_blob_out, processed_manifest_out)
    # ))
    #
    # pending_blob_in = pending_blob_out
    # processed_blob_out = asyncio.Queue()  # blobs don't ref other units, so dl == process
    # blob_download_stage = ArtifactDownloader()
    # futures.append(asyncio.ensure_future(
    #     blob_download_stage(pending_blob_in, processed_blob_out)
    # ))
    #
    # # confluence....
    # dc_in_qs = [
    #     processed_tag_out,
    #     processed_manifest_list_out,
    #     processed_manifest_out,
    #     processed_blob_out,
    # ]
    # dc_out = asyncio.Queue()
    # dc_confluence = ConfluenceStage()
    # futures.append(asyncio.ensure_future(
    #     dc_confluence(dc_in_qs, dc_out)
    # ))
    #
    # dc_in = dc_out
    # dc_saved_artifacts = asyncio.Queue()
    # save_artifacts_stage = QueryAndSaveArtifacts()
    # futures.append(asyncio.ensure_future(
    #     save_artifacts_stage(dc_in, dc_saved_artifacts)
    # ))
    #
    # dc_unsaved_in = dc_saved_artifacts
    # dc_saved_out = asyncio.Queue()
    # content_saver_stage = ContentUnitSaver()
    # futures.append(asyncio.ensure_future(
    #     content_saver_stage(dc_unsaved_in, dc_saved_out)
    # ))
    #
    # dc_not_associated_in = dc_saved_out
    # dc_associated_out = asyncio.Queue()
    # association_stage = ContentUnitAssociation(new_version)
    # futures.append(asyncio.ensure_future(
    #     association_stage(dc_not_associated_in, dc_associated_out)
    # ))
    #
    # dc_not_unassociated_in = dc_associated_out
    # fin_out = asyncio.Queue()
    # unassociation_stage = ContentUnitUnassociation(new_version)
    # futures.append(asyncio.ensure_future(
    #     unassociation_stage(dc_not_unassociated_in, fin_out)
    # ))
    #
    # fin_in = fin_out
    # end_stage = EndStage()
    # futures.append(asyncio.ensure_future(end_stage(fin_in)))
    #
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
    # in_q = tag_out
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
