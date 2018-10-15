"""
The purpose of these stages is to mock out a pattern for how stages can emit to more than one
out_q, and other stages can recieve input from more than one in_q.

To start with, we need to create a stage that emits 2 outputs.
  - each q needs to be created ahead of time by the pipeline.
  - pass the qs into the stage as it's future is ensured
  - ensure_future adds the stage to the event loop
"""
from urllib.parse import urljoin
import json

from pulpcore.plugin.models import (Artifact, ProgressBar,
                                    ContentArtifact)# noqa

from pulp_docker.app.models import (ImageManifest, MEDIA_TYPE, ManifestBlob,
                                    ManifestList, Tag, BlobManifestBlob, ManifestListManifest)
from pulp_docker.app.tasks.synchronizing import V2_ACCEPT_HEADERS

from pulpcore.plugin.stages import (
    # ArtifactDownloaderRunner,
    DeclarativeArtifact,
    DeclarativeContent,
    Stage
)


import logging
log = logging.getLogger("CONFLUENCE SANITY STAGES")

tag_log = logging.getLogger("TAGLIST")


class V2_API:

    @staticmethod
    def tags_list_url(remote):
        relative_url = '/v2/{name}/tags/list'.format(name=remote.namespaced_upstream_name)
        return urljoin(remote.url, relative_url)

    @staticmethod
    def get_tag_url(remote, tag):
        relative_url = '/v2/{name}/manifests/{tag}'.format(
            name=remote.namespaced_upstream_name,
            tag=tag
        )
        return urljoin(remote.url, relative_url)


class RemoteStage(Stage):
    """
    Stage aware of a docker remote.
    """
    def __init__(self, remote):
        self.remote = remote
        self.extra_request_data = {'headers': V2_ACCEPT_HEADERS}
        # TODO(use or lose)
        self.docker_api = V2_API


class TagListStage(RemoteStage):
    """
    The first stage of a pulp_docker sync pipeline.
    """
    # TODO(asmacdo) self.remote is needed for Declarative artifacts, so needed by all plugins.
    # Add this to the base class?

    async def __call__(self, unused_in_q, pending_tag_out):
        """
        Build and emit `DeclarativeContent` from the Manifest data.

        Args:
            in_q (asyncio.Queue): Unused because the first stage doesn't read from an input queue.
            out_q (asyncio.Queue): The out_q to send `DeclarativeContent` objects to

        """
        tag_log.info('starting tag list')
        with ProgressBar(message="Downloading Tags List") as pb:
            tag_log.info("Fetching tags list for upstream repository: {repo}".format(
                repo=self.remote.upstream_name
            ))
            relative_url = '/v2/{name}/tags/list'.format(name=self.remote.namespaced_upstream_name)
            tag_list_url = urljoin(self.remote.url, relative_url)
            list_downloader = self.remote.get_downloader(tag_list_url)
            await list_downloader.run()

        with open(list_downloader.path) as tags_raw:
            tags_dict = json.loads(tags_raw.read())
            tag_list = tags_dict['tags']
        pb.increment()
        for tag_name in tag_list:
            await self.create_pending_tag(tag_name, pending_tag_out)
            pb.increment()

        # tag_log.warn("Skipped types: {ctypes}".format(ctypes=self._log_skipped_types))
        await pending_tag_out.put(None)

    async def create_pending_tag(self, tag_name, pending_tag_out):
        tag = Tag(name=tag_name)
        url = V2_API.get_tag_url(self.remote, tag_name)
        # Create now so downloader will download it, we'll pop it off later.
        manifest_artifact = Artifact()  # ??? size path digests???
        da = DeclarativeArtifact(
            artifact=manifest_artifact,
            url=url,
            relative_path="TODO-where-should-this-go-{name}".format(name=tag_name),
            remote=self.remote,
            extra_data=self.extra_request_data,
        )
        tag_dc = DeclarativeContent(content=tag, d_artifacts=[da])
        await pending_tag_out.put(tag_dc)


class ProcessContentStage(RemoteStage):
    """
        process_tag_stage(unprocessed_tag_in, unprocessed_manifest_list_out,
                          unprocessed_manifest_out1, processed_tag_out)

    If Tag:
        if manifest_list
            create the manifest list
            create pending manifests
        if manifest
            create manifest
            create pending blobs
    if manifest_list (shouldnt happen)
    if manifest
        create pending blobs
    """
    async def __call__(self, in_q, out_q):
        while True:
            dc = await in_q.get()
            if dc is None:
                break
            elif dc.content.pk is not None:
                # If content has been saved, it has also been processed.
                await out_q.put(dc)
                continue
            elif type(dc.content) is ManifestBlob:
                # We don't need to/can't read blobs.
                await out_q.put(dc)
                continue

            # TODO
            # assert len(dc.d_artifacts) == 1
            with dc.d_artifacts[0].artifact.file.open() as content_file:
                raw = content_file.read()
            content_data = json.loads(raw)
            # TODO this emits the new content, but does not relate them. How can we
            # store that relation to be made after all units have been saved?
            #      - Idea: put the Tag class and the tag pk into the
            #      DeclarativeArtifact.extra_data.
            #      - ^ Nope, we don't have pk until the tag is saved.
            #      - One way to do this:
            #          - the Tag and the [Manifest|ManifestList] share an artifact,
            #          - that artifact has a digest, which is the pk of both.
            #              Tag.objects.get(pk=digest) Manifest.objects.get(pk=digest)
            if type(dc.content) is Tag:
                if content_data.get('mediaType') == MEDIA_TYPE.MANIFEST_LIST:
                    await self.create_and_process_tagged_manifest_list(dc, content_data, out_q)
                elif content_data.get('mediaType') == MEDIA_TYPE.MANIFEST_V2:
                    await self.create_and_process_tagged_manifest(dc, content_data, out_q)
            elif type(dc.content) is ImageManifest:
                for layer in content_data.get("layers"):
                    await self.create_pending_blob(dc, layer, out_q)
                await out_q.put(dc)
            elif type(dc.content) is ManifestList:
                import ipdb; ipdb.set_trace()
                tag_log.warn("MANIFEST SHOULD ALREADY BE SAVED")

                for manifest in content_data.get("manifests"):
                    await self.create_pending_manifest(dc, manifest, out_q)
                await out_q.put(dc)

        await out_q.put(None)

    async def create_and_process_tagged_manifest_list(self, tag_dc, manifest_list_data, out_q):
        # TODO(test this) dc that comes here should always be a tag
        manifest_list = ManifestList(
            # TODO might need to be sha256:asdfasd
            digest="sha256:{digest}".format(digest=tag_dc.d_artifacts[0].artifact.sha256),
            schema_version=manifest_list_data['schemaVersion'],
            media_type=manifest_list_data['mediaType'],
        )
        tag_dc.content.manifest_list = manifest_list
        # extra_data="TODO(asmacdo) add reference to tag"
        list_dc = DeclarativeContent(content=manifest_list, d_artifacts=[tag_dc.d_artifacts[0]])
        # self.ml_num += 1
        # tag_log.warn("New manifest list")
        for manifest in manifest_list_data.get('manifests'):
            await self.create_pending_manifest(list_dc, manifest, out_q)

        await out_q.put(list_dc)

    async def create_and_process_tagged_manifest(self, dc, manifest_data, out_q):
        # tagged manifests actually have an artifact already that we need to use.
        manifest = ImageManifest(
            digest=dc.d_artifacts[0].artifact.sha256,
            schema_version=manifest_data['schemaVersion'],
            media_type=manifest_data['mediaType'],
        )
        # extra_data="TODO(asmacdo) add reference to tag"
        man_dc = DeclarativeContent(content=manifest, d_artifacts=[dc.d_artifacts[0]])
        for layer in manifest_data.get('layers'):
            await self.create_pending_blob(man_dc, layer, out_q)
        await out_q.put(man_dc)

    async def create_pending_manifest(self, list_dc, manifest_data, out_q):
        digest = manifest_data['digest']
        relative_url = '/v2/{name}/manifests/{digest}'.format(
            name=self.remote.namespaced_upstream_name,
            digest=digest
        )
        manifest_url = urljoin(self.remote.url, relative_url)
        manifest_artifact = Artifact()  # ??? size path digests???
        da = DeclarativeArtifact(
            artifact=manifest_artifact,
            url=manifest_url,
            relative_path=digest,
            remote=self.remote,
            extra_data=self.extra_request_data,
            # extra_data="TODO(asmacdo) add reference to manifest list"
        )
        manifest = ImageManifest(
            digest=manifest_data['digest'],
            schema_version=2,
            media_type=manifest_data['mediaType'],
        )
        man_dc = DeclarativeContent(content=manifest, d_artifacts=[da])
        man_dc.extra_data['relations'] = [list_dc]
        await out_q.put(man_dc)

    async def create_pending_blob(self, man_dc, blob_data, out_q):
        sha256 = blob_data['digest'],
        # TODO are Artifact fields actually helpful in any way?
        blob_artifact = Artifact()
        blob = ManifestBlob(
            digest=sha256,
            media_type=blob_data['mediaType'],
        )
        relative_url = '/v2/{name}/blobs/{digest}'.format(
            name=self.remote.namespaced_upstream_name,
            digest=blob_data['digest'],
        )
        blob_url = urljoin(self.remote.url, relative_url)
        da = DeclarativeArtifact(
            artifact=blob_artifact,
            url=blob_url,
            relative_path=blob_data['digest'],
            remote=self.remote,
            extra_data=self.extra_request_data,
            # extra_data="TODO(asmacdo) add reference to manifest"
        )
        blob_dc = DeclarativeContent(content=blob, d_artifacts=[da])
        blob_dc.extra_data['relations'] = [man_dc]
        await out_q.put(blob_dc)


class InterrelateContent(Stage):
    async def __call__(self, in_q, out_q):
        while True:
            dc = await in_q.get()
            if dc is None:
                break
            # TODO don't assume all extra data is relations
            if dc.extra_data:
                if type(dc.content) is ManifestBlob:
                    self.interrelate_blob(dc)
                elif type(dc.content) is ImageManifest:
                    self.interrelate_manifest(dc)
            await out_q.put(dc)
        await out_q.put(None)

    def interrelate_blob(self, dc):
        relations = dc.extra_data.get('relations', [])
        # TODO we need to merge relations at dedup & save
        for related_dc in relations:
            try:
                BlobManifestBlob.objects.get(manifest=related_dc.content, manifest_blob=dc.content)
            except BlobManifestBlob.DoesNotExist:
                thru = BlobManifestBlob(manifest=related_dc.content, manifest_blob=dc.content)
                thru.save()
            else:
                pass

    def interrelate_manifest(self, dc):
        relations = dc.extra_data.get('relations', [])
        # TODO we need to merge relations at dedup & save
        for related_dc in relations:
            try:
                ManifestListManifest.objects.get(
                    manifest_list=related_dc.content, manifest=dc.content)
            except ManifestListManifest.DoesNotExist:
                thru = ManifestListManifest(manifest_list=related_dc.content, manifest=dc.content)
                thru.save()
            else:
                pass


class DidItWorkStage(Stage):
    async def __call__(self, in_q, out_q):
        while True:
            log_it = await in_q.get()
            if log_it is None:
                break
            self.log_state(log_it)
            await out_q.put(log_it)
        await out_q.put(None)

    def log_state(self, dc):
        # TODO dont assume 1 artifact
        downloaded = dc.d_artifacts[0].artifact.file.name != ""
        dl = "D" if downloaded else "!d"
        a_saved = dc.d_artifacts[0].artifact.pk is not None
        a_s = "A" if a_saved else "!a"
        saved = dc.content.pk is not None
        sa = "S" if saved else "!s"
        log.info("{dct}: {dl}{a_s}{sa}".format(dct=type(dc.content), dl=dl, a_s=a_s, sa=sa))
