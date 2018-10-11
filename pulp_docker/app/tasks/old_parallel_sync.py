from gettext import gettext as _
from urllib.parse import urljoin
import asyncio
import base64
import hashlib
import json
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository  # noqa
from pulpcore.plugin.stages import (
    ArtifactDownloaderRunner,
    ArtifactDownloader,
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    Stage
)

# TODO(asmacdo) alphabetize
from pulp_docker.app.models import (ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob, Tag,
                                    ManifestList)


log = logging.getLogger(__name__)


V2_ACCEPT_HEADERS = {
    'accept': ','.join([MEDIA_TYPE.MANIFEST_V2, MEDIA_TYPE.MANIFEST_LIST])
}


def synchronize(remote_pk, repository_pk):
    """
    Sync content from the remote repository.

    Create a new version of the repository that is synchronized with the remote.

    Args:
        remote_pk (str): The remote PK.
        repository_pk (str): The repository PK.

    Raises:
        ValueError: If the remote does not specify a URL to sync

    """
    remote = DockerRemote.objects.get(pk=remote_pk)
    repository = Repository.objects.get(pk=repository_pk)

    first_stage = DockerTagListStage(remote)
    wait_q = asyncio.Queue()
    rec_q = asyncio.Queue()
    second_stage = RecursiveQueue(wait_q, rec_q)
    third_stage = DockerArtifactDownloader(wait_q, rec_q)
    stages = [first_stage, second_stage, third_stage]
    DockerDeclarativeVersion(repository, stages=stages).create()


class WaitOnMe:
    pass


class DockerTagListStage(Stage):
    """
    The first stage of a pulp_docker sync pipeline.
    """

    # TODO(asmacdo) self.remote is needed for Declarative artifacts, so needed by all plugins.
    # Add this to the base class?
    def __init__(self, remote):
        """
        The first stage of a pulp_docker sync pipeline.

        Args:
            remote (DockerRemote): The remote data to be used when syncing

        """
        self.remote = remote

    async def __call__(self, in_q, out_q):
        """
        Build and emit `DeclarativeContent` from the Manifest data.

        Args:
            in_q (asyncio.Queue): Unused because the first stage doesn't read from an input queue.
            out_q (asyncio.Queue): The out_q to send `DeclarativeContent` objects to

        """
        with ProgressBar(message="Downloading Tags") as pb:
            log.info("Fetching tags list for upstream repository: {repo}".format(
                repo=self.remote.upstream_name
            ))
            list_downloader = self.remote.get_downloader(self.tags_list_url)
            await list_downloader.run()

            with open(list_downloader.path) as tags_raw:
                tags_dict = json.loads(tags_raw.read())
            tag_list = tags_dict['tags']
            # pb.increment()
            for tag_name in tag_list[0:3]:
                tag = Tag(name=tag_name)
                # Artifact will not actually belong to the tag, but will be converted into
                # content (manifest or manifest list) and this artifact will belong to that
                # content.
                # TODO(asmacdo) we know nothing
                manifest_artifact = Artifact()
                da = DeclarativeArtifact(
                    artifact=manifest_artifact,
                    url=self.get_tag_url(tag_name),
                    # TODO(asmacdo) idk?
                    relative_path=tag_name,
                    remote=self.remote,
                    extra_data={'headers': V2_ACCEPT_HEADERS},
                )
                dc = DeclarativeContent(content=tag, d_artifacts=[da])
                log.info("EMIT tag name {tag}".format(tag=tag_name))
                await out_q.put(dc)
                pb.increment()
        await out_q.put(None)

    @property
    def tags_list_url(self):
        relative_url = '/v2/{name}/tags/list'.format(name=self.remote.namespaced_upstream_name)
        return urljoin(self.remote.url, relative_url)

    def get_tag_url(self, tag):
        relative_url = '/v2/{name}/manifests/{tag}'.format(
            name=self.remote.namespaced_upstream_name,
            tag=tag
        )
        return urljoin(self.remote.url, relative_url)


class RecursiveQueue(Stage):

    def __init__(self, wait_q, rec_q):
        self.wait_q = wait_q
        self.rec_q = rec_q

    async def __call__(self, in_q, out_q):
        log.info("Recursive Queue STAGE")
        prev_running = True
        while prev_running:
            to_log = await in_q.get()
            if to_log is not None:
                self.wait_q.put(WaitOnMe)
                log.info("echo Emit {out}".format(out=to_log))
                out_q.put(to_log)
            else:
                log.info('prev finished')
                prev_running = False

        while True:
            try:
                to_log = await self.rec_q.get_nowait()
            except Exception as e:
                log.warn(e)
                if self.wait_q.empty():
                    log.warn("Empty wait, break")
                    break

            if to_log is not None:
                self.wait_q.put(WaitOnMe)
                log.info("Recursive Emit {out}".format(out=to_log))
                out_q.put(to_log)

        log.warn("FINISH recursive")
        await out_q.put(None)


class DockerArtifactDownloader(ArtifactDownloader):
    def __init__(self, wait_q, rec_q, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wait_q = wait_q
        self.rec_q = rec_q

    async def __call__(self, in_q, out_q):
        """
        The coroutine for this stage.

        Args:
            in_q (:class:`asyncio.Queue`): The queue to receive
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects from that may have
                undownloaded files.
            out_q (:class:`asyncio.Queue`): The queue to put
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects into, all of which have
                files downloaded.

        Returns:
            The coroutine for this stage.
        """
        runner = RecursiveArtifactDLRunner(self.wait_q, self.rec_q, in_q, out_q, self.max_concurrent_downloads,
                                           self.max_concurrent_content)
        await runner.run()


class RecursiveArtifactDLRunner(ArtifactDownloaderRunner):
    def __init__(self, wait_q, rec_qq, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wait_q = wait_q

    async def run(self):
        """
        The coroutine doing the stage's work.
        """
        #: (set): The set of unfinished tasks.  Contains the content
        #    handler tasks and may contain `self._content_get_task`.
        self._pending = set()

        #: (:class:`asyncio.Task`): The task that gets new content from `in_q`.
        #    Set to None if stage is shutdown.
        self._content_get_task = self._add_to_pending(self.in_q.get())
        log.warn("Artifact DLer starting up")

        #: (:class:`asyncio.Semaphore`): Semaphore controlling the number of concurrent downloads
        self._download_semaphore = asyncio.Semaphore(value=self.max_concurrent_downloads)

        with ProgressBar(message='Downloading Artifacts') as pb:
            try:
                while self._pending:
                    done, self._pending = await asyncio.wait(self._pending,
                                                             return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        if task is self._content_get_task:
                            content = task.result()
                            self.wait_q.get()
                            if content is None:
                                # previous stage is finished and we retrieved all
                                # content instances: shutdown
                                self._content_get_task = None
                            else:
                                log.info("Pending: handle new unit")
                                self._add_to_pending(self._handle_content_unit(content))
                        else:
                            download_count = task.result()
                            pb.done += download_count
                            pb.save()

                    if not self.shutdown:
                        if not self.saturated and self._content_get_task not in self._pending:
                            log.info("Pending: Adding next in_q.get()")
                            self._content_get_task = self._add_to_pending(self.in_q.get())
            except asyncio.CancelledError:
                # asyncio.wait does not cancel its tasks when cancelled, we need to do this
                for future in self._pending:
                    future.cancel()
                raise

        log.warn("Artifact DLer shutting down")
        await self.out_q.put(None)

    def _update_content(self, content, downloads):
        """Update the content using the download results."""
        MAN_LIST = 'application/vnd.docker.distribution.manifest.list.v2+json'
        MANIFEST = 'application/vnd.docker.distribution.manifest.v2+json'
        BLOB = 'TODO'
        for download_result in downloads:
            def url_lookup(x):
                return x.url == download_result.url
            log.info("Updating content {content}".format(content=content))
            d_artifact = list(filter(url_lookup, content.d_artifacts))[0]
            if d_artifact.artifact.pk is None:
                with open(download_result.path) as file:
                    data = json.loads(file.read())

                schema_version = data.get('schemaVersion')
                if schema_version == 1:
                    log.info("Skipping v1, {url}".format(url=download_result.url))
                    continue

                media_type = data.get('mediaType')
                if media_type == MAN_LIST:
                    self.handle_manifest_list(content, download_result, data)
                elif media_type == MANIFEST:
                    self.handle_manifest(content, download_result, data)
                elif media_type == BLOB:
                    self.handle_blob(content, download_result, data)
                else:
                    log.warn("NOT SET UP")
                    log.warn(media_type)
                    print('x')

                # new_artifact = Artifact(
                #     **download_result.artifact_attributes,
                #     file=download_result.path
                # )
                # d_artifact.artifact = new_artifact

    def handle_manifest_list(self, content, download_result, data):
        remote = content.d_artifacts[0].remote

        new_artifact = Artifact(
            **download_result.artifact_attributes,
            file=download_result.path
        )

        for manifest_data in data.get('manifests', []):
            manifest = ImageManifest(
                digest=manifest_data['digest'],
                schema_version=2,
                media_type=manifest_data['mediaType'],
            )
            da = DeclarativeArtifact(
                artifact=new_artifact,
                url=self.get_tag_url(remote, manifest_data['digest']),
                relative_path=manifest_data['digest'],
                remote=remote,
                extra_data={'headers': V2_ACCEPT_HEADERS},
            )
            dc = DeclarativeContent(content=manifest, d_artifacts=[da])
            log.warn("put to rec_q")
            self.rec_q.put(dc)

        # TODO(asmacdo) tags shouldn't have artifacts
        # if isinstance(content.content, Tag):
        #     content.d_artifact = None

    def handle_manifest(self, content, download_result, data):
        log.warn(data)
        log.warn("SKIPPING MANIFEST")

    def handle_blob(self, content, download_result, data):
        log.warn("SKIPPING BLOB")

    def get_tag_url(self, remote, tag):
        relative_url = '/v2/{name}/manifests/{tag}'.format(
            name=remote.namespaced_upstream_name,
            tag=tag
        )
        return urljoin(remote.url, relative_url)


class DockerDeclarativeVersion(DeclarativeVersion):

    def __init__(self, repository, mirror=None, stages=None):
        self.stages = stages or []
        self.repository = repository
        self.mirror = mirror

    def pipeline_stages(self, new_version, stages=None):
        """
        Build the list of pipeline stages feeding into the
        ContentUnitAssociation stage.

        Plugin-writers may override this method to build a custom pipeline. This
        can be achieved by returning a list with different stages or by extending
        the list returned by this method.

        Args:
            new_version (:class:`~pulpcore.plugin.models.RepositoryVersion`): The
                new repository version that is going to be built.

        Returns:
            list: List of :class:`~pulpcore.plugin.stages.Stage` instances

        """
        return [stage for stage in self.stages]
