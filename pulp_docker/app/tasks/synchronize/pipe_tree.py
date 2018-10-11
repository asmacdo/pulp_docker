import asyncio


class StageNode:
    def __init__(self, stage):
        self.stage = stage
        self.in_qs = {}
        self.out_qs = {}
        self.children = []
        self._ensured = False

    def add_child(self, q_name, child_stage):
        connecting_q = asyncio.Queue()
        child_stage.in_qs[q_name] = connecting_q
        self.out_qs[q_name] = connecting_q
        self.children.append(child_stage)

    def ensure(self, futures):
        if not self._ensured:
            if len(self.qs.keys()) > 1:
                self.add_confluence_parent()
            else:
                futures.append(asyncio.ensure_future(self.stage(**self.in_qs, **self.out_qs)))
            self._ensured = True
            for child in self.children:
                child.ensure(futures)
        return self._ensured

    def add_confluence_parent(self, futures):
        confluence = ConfluenceStage()
        single_stream_out = asyncio.Queue()
        futures.append(asyncio.ensure_future(confluence(single_stream_out, **self.in_qs)))
        single_stream_in = single_stream_out
        futures.append(asyncio.ensure_future(self.stage(single_stream_in, **self.out_qs)))

tag_list_stage = StageNode(TagListStage(remote))
tag_download_stage = StageNode(ArtifactDownloader())
process_tag_stage = StageNode(ProcessTagStage(remote=remote))
process_manifest_list_stage = ProcessManifestListStage()
manifest_download_stage = ArtifactDownloader()
process_manifest_stage = ProcessManifestStage()
# lazy will be the same until here
blob_download_stage = ArtifactDownloader()
save_artifacts_stage = QueryAndSaveArtifacts()
content_saver_stage = ContentUnitSaver()
association_stage = ContentUnitAssociation(new_version)
unassociation_stage = ContentUnitUnassociation(new_version)
end_stage = EndStage()





tag_list.add_child("pending_tags", tag_download_stage,)

tag_download_stage.add(process_tag_stage)

process_tag_stage.add_child("
    # pending_tag_out = asyncio.Queue(maxsize=maxsize)
    # tag_list_stage = TagListStage(remote)
    # futures.append(asyncio.ensure_future(tag_list_stage(pending_tag_out)))
    #
    # Tags should always replace previous tags, dont queryexisting?
    # query_existing = QueryExistingArtifacts()
    #
    # pending_tag_in = pending_tag_out
    # unprocessed_tag_out = asyncio.Queue(maxsize=maxsize)
    # tag_download_stage = ArtifactDownloader()
    # futures.append(asyncio.ensure_future(
    #     tag_download_stage(pending_tag_in, unprocessed_tag_out))
    # )
    #
    # unprocessed_tag_in = unprocessed_tag_out
    # unprocessed_manifest_list_out = asyncio.Queue()
    # unprocessed_manifest_out1 = asyncio.Queue()
    # processed_tag_out = asyncio.Queue()
    # process_tag_stage = ProcessTagStage(remote=remote)
    # futures.append(asyncio.ensure_future(
    #     process_tag_stage(unprocessed_tag_in, unprocessed_manifest_list_out,
    #                       unprocessed_manifest_out1, processed_tag_out)
    # ))
    #
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
    end_stage = EndStage()
    # futures.append(asyncio.ensure_future(end_stage(fin_in)))
    #
    #
    #
    #




