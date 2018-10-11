"""
The purpose of these stages is to mock out a pattern for how stages can emit to more than one
out_q, and other stages can recieve input from more than one in_q.

To start with, we need to create a stage that emits 2 outputs.
  - each q needs to be created ahead of time by the pipeline.
  - pass the qs into the stage as it's future is ensured
  - ensure_future adds the stage to the event loop
"""
from urllib.parse import urljoin
import asyncio
import json

from django.db.models import Q

from pulpcore.plugin.models import Artifact, ProgressBar, Repository, RepositoryVersion  # noqa

from pulp_docker.app.models import (ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob,
                                    ManifestList, Tag)
from pulp_docker.app.tasks.synchronizing import V2_ACCEPT_HEADERS

from pulpcore.plugin.stages import (
    ArtifactDownloaderRunner,
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
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


class ConfluenceStage(Stage):
    """
    Waits on an arbitrary number of input queues and outputs as single queue.
    """
    async def __call__(self, in_q_list, joined_out):
        self._pending = set()
        self._finished = set()
        open_queues = [None for q in in_q_list]
        current_tasks = {}
        for queue in in_q_list:
            task = self.add_to_pending(queue.get())
            current_tasks[task] = queue

        while open_queues:
            done, self._pending = await asyncio.wait(
                self._pending,
                return_when=asyncio.FIRST_COMPLETED
            )
            self._finished = self._finished.union(done)

            while self._finished:
                out_task = self._finished.pop()
                if out_task.result() is None:
                    open_queues.pop()
                else:
                    used_queue = current_tasks.pop(out_task)
                    next_task = self.add_to_pending(used_queue.get())
                    current_tasks[next_task] = used_queue
                    await joined_out.put(out_task.result())
        # After both inputs are finished (2 Nones) we close this stage
        await joined_out.put(None)

    def add_to_pending(self, coro):
        task = asyncio.ensure_future(coro)
        self._pending.add(asyncio.ensure_future(task))
        return task


class QueryAndSaveArtifacts(Stage):
    """
    The stage that bulk saves only the artifacts that have not been saved before.
    A stage that replaces :attr:`DeclarativeContent.d_artifacts` objects with
    already-saved :class:`~pulpcore.plugin.models.Artifact` objects.
    This stage expects :class:`~pulpcore.plugin.stages.DeclarativeContent` units from `in_q` and
    inspects their associated :class:`~pulpcore.plugin.stages.DeclarativeArtifact` objects. Each
    :class:`~pulpcore.plugin.stages.DeclarativeArtifact` object stores one
    :class:`~pulpcore.plugin.models.Artifact`.
    This stage inspects any unsaved :class:`~pulpcore.plugin.models.Artifact` objects and searches
    using their metadata for existing saved :class:`~pulpcore.plugin.models.Artifact` objects inside
    Pulp with the same digest value(s). Any existing :class:`~pulpcore.plugin.models.Artifact`
    objects found will replace their unsaved counterpart in the
    :class:`~pulpcore.plugin.stages.DeclarativeArtifact` object. Each remaining unsaved
    :class:`~pulpcore.plugin.models.Artifact` is saved.
    Each :class:`~pulpcore.plugin.stages.DeclarativeContent` is sent to `out_q` after all of its
    :class:`~pulpcore.plugin.stages.DeclarativeArtifact` objects have been handled.
    This stage drains all available items from `in_q` and batches everything into one large call to
    the db for efficiency.
    """

    async def __call__(self, in_q, out_q):
        """
        The coroutine for this stage.
        Args:
            in_q (:class:`asyncio.Queue`): The queue to receive
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects from.
            out_q (:class:`asyncio.Queue`): The queue to put
                :class:`~pulpcore.plugin.stages.DeclarativeContent` into.
        Returns:
            The coroutine for this stage.
        """
        async for batch in self.batches(in_q):
            all_artifacts_q = Q(pk=None)
            for content in batch:
                for declarative_artifact in content.d_artifacts:
                    one_artifact_q = Q()
                    for digest_name in declarative_artifact.artifact.DIGEST_FIELDS:
                        digest_value = getattr(declarative_artifact.artifact, digest_name)
                        if digest_value:
                            key = {digest_name: digest_value}
                            one_artifact_q &= Q(**key)
                    if one_artifact_q:
                        all_artifacts_q |= one_artifact_q

            for artifact in Artifact.objects.filter(all_artifacts_q):
                for content in batch:
                    for declarative_artifact in content.d_artifacts:
                        for digest_name in artifact.DIGEST_FIELDS:
                            digest_value = getattr(declarative_artifact.artifact, digest_name)
                            if digest_value and digest_value == getattr(artifact, digest_name):
                                declarative_artifact.artifact = artifact
                                break

            artifacts_to_save = []

            for declarative_content in batch:
                for declarative_artifact in declarative_content.d_artifacts:
                    if declarative_artifact.artifact.pk is None:
                        declarative_artifact.artifact.file = str(
                            declarative_artifact.artifact.file)
                        artifacts_to_save.append(declarative_artifact.artifact)

            if artifacts_to_save:
                Artifact.objects.bulk_create(artifacts_to_save)

            for declarative_content in batch:
                await out_q.put(declarative_content)
        await out_q.put(None)


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

    async def __call__(self, pending_tag_out):
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
            list_downloader = self.remote.get_downloader(V2_API.tags_list_url(self.remote))
            await list_downloader.run()

        with open(list_downloader.path) as tags_raw:
            tags_dict = json.loads(tags_raw.read())
            tag_list = tags_dict['tags']
        pb.increment()
        for tag_name in tag_list:
            await self.create_and_emit_tag(tag_name, pending_tag_out)
            pb.increment()

        # tag_log.warn("Skipped types: {ctypes}".format(ctypes=self._log_skipped_types))
        await pending_tag_out.put(None)

    async def create_and_emit_tag(self, tag_name, pending_tag_out):
        tag = Tag(name=tag_name)
        # Create now so downloader will download it, we'll pop it off later.
        manifest_artifact = Artifact()  # ??? size path digests???
        da = DeclarativeArtifact(
            artifact=manifest_artifact,
            url=V2_API.get_tag_url(self.remote, tag_name),
            relative_path="TODO-where-should-this-go-{name}".format(name=tag_name),
            remote=self.remote,
            extra_data=self.extra_request_data,
        )
        tag_dc = DeclarativeContent(content=tag, d_artifacts=[da])
        await pending_tag_out.put(tag_dc)


class ProcessTagStage(Stage):
    """
        process_tag_stage(unprocessed_tag_in, unprocessed_manifest_list_out,
                          unprocessed_manifest_out1, processed_tag_out)
    """
    def __init__(self, remote):
        self.remote = remote

    async def __call__(self, unprocessed_tag_in, processed_manifest_list_out,
                       unprocessed_manifest_out, processed_tag_out):
        tag_log.info('process tag START')
        while True:
            tag_dc = await unprocessed_tag_in.get()
            if tag_dc is None:
                break
            tag_log.info("NOT BROKEN HERE")
            assert len(tag_dc.d_artifacts) == 1
            with tag_dc.d_artifacts[0].artifact.file.open() as manifest_file:
                raw = manifest_file.read()
            manifest_data = json.loads(raw)
            # TODO this emits the new content, but does not relate them. How can we
            # store that relation to be made after all units have been saved?
            #      - Idea: put the Tag class and the tag pk into the
            #      DeclarativeArtifact.extra_data.
            #      - ^ Nope, we don't have pk until the tag is saved.
            #      - One way to do this:
            #          - the Tag and the [Manifest|ManifestList] share an artifact,
            #          - that artifact has a digest, which is the pk of both.
            #              Tag.objects.get(pk=digest) Manifest.objects.get(pk=digest)
            if manifest_data.get('mediaType') == MEDIA_TYPE.MANIFEST_LIST:
                self.create_and_emit_manifest_list(tag_dc, manifest_data, processed_manifest_out1)
            elif manifest_data.get('mediaType') == MEDIA_TYPE.MANIFEST_V2:
                self.create_and_emit_manifest(manifest_data, unprocessed_manifest_list_out)

        await unprocessed_manifest_list_out.put(None)
        await unprocessed_manifest_out1.put(None)
        await processed_tag_out.put(None)

    # def create_and_emit_manifest(manifest_data, unprocessed_manifest_out1):
    def create_and_emit_manifest_list(self, tag_dc, manifest_list_data, processed_manifest_list_out):
        manifest_list = ManifestList(
            # TODO might need to be sha256:asdfasd
            digest=tag_dc.d_artifacts[0].artifact.sha256,
            schema_version=manifest_list_data['schemaVersion'],
            media_type=manifest_list_data['mediaType'],
        )
        list_dc = DeclarativeContent(content=manifest_list, d_artifacts=[tag_dc.d_artifacts[0]])
        processed_manifest_list_out.put(list_dc)


class ProcessManifestStage(Stage):
    """
        process_manifest_stage(unprocessed_manifest_in, pending_blob_out, processed_manifest_out)
    """


class DidItWorkStage(Stage):
    async def __call__(self, in_q):
        while True:
            log_it = await in_q.get()
            if log_it is not None:
                log.info(log_it)
            else:
                break
