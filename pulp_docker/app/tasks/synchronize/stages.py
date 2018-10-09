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
                log.info(log_it.d_artifacts)
            else:
                break


class TagListStage(Stage):
    """
    The first stage of a pulp_docker sync pipeline.
    """

    # TODO(asmacdo) self.remote is needed for Declarative artifacts, so needed by all plugins.
    # Add this to the base class?
    def __init__(self, remote, tag_out_queue):
        """
        The first stage of a pulp_docker sync pipeline.

        Args:
            remote (DockerRemote): The remote data to be used when syncing

        """
        self.remote = remote
        self.extra_request_data = {'headers': V2_ACCEPT_HEADERS}
        self._log_skipped_types = set()
        self.tag_out = tag_out_queue

    async def __call__(self):
        """
        Build and emit `DeclarativeContent` from the Manifest data.

        Args:
            in_q (asyncio.Queue): Unused because the first stage doesn't read from an input queue.
            out_q (asyncio.Queue): The out_q to send `DeclarativeContent` objects to

        """
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
                await self.process_and_emit_tag(tag_name)
                pb.increment()

        tag_log.warn("Skipped types: {ctypes}".format(ctypes=self._log_skipped_types))
        await self.tag_out.put(None)

    async def process_and_emit_tag(self, tag_name):
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
        await self.tag_out.put(tag_dc)


