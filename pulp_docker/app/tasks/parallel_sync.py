from gettext import gettext as _
from urllib.parse import urljoin
import asyncio
import json
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository, RepositoryVersion  # noqa
from pulpcore.plugin.stages import (
    ContentUnitAssociation,
    ContentUnitUnassociation,
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    EndStage,
    Stage
)
from pulpcore.plugin.tasking import WorkingDirectory

# TODO(asmacdo) alphabetize
from pulp_docker.app.models import (ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob,
                                    ManifestList, Tag)


# log = logging.getLogger(__name__)
tag_log = logging.getLogger("TAG")
list_log = logging.getLogger("****MANIFEST_LIST")
man_log = logging.getLogger("--------MANIFEST")
blob_log = logging.getLogger("++++++++++++++++BLOB")


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

    if not remote.url:
        raise ValueError(_('A remote must have a url specified to synchronize.'))

    # # stages = self.pipeline_stages(new_version)
    # stages.append(ContentUnitAssociation(new_version))
    # if self.mirror:
    #     stages.append(ContentUnitUnassociation(new_version))
    # stages.append(EndStage())
    pipeline = create_sync_pipeline()
    DockerDeclarativeVersion(repository, pipeline).create()


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





class DockerFirstStage(Stage):
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
        self.extra_request_data = {'headers': V2_ACCEPT_HEADERS}
        self._log_skipped_types = set()

    async def __call__(self, in_q, out_q):
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
            list_downloader = self.remote.get_downloader(self.tags_list_url)
            await list_downloader.run()

            with open(list_downloader.path) as tags_raw:
                tags_dict = json.loads(tags_raw.read())
                tag_list = tags_dict['tags']
            pb.increment()
        with ProgressBar(message="Downloading Tagged Manifests and Manifest Lists") as pb:
            for tag_name in tag_list:
                out_name = await self.download_and_process_tag(tag_name, out_q)
                tag_log.info("Done waiting on tag {name}".format(name=out_name))
                pb.increment()

        tag_log.warn("Skipped types: {ctypes}".format(ctypes=self._log_skipped_types))
        await out_q.put(None)

    async def download_and_process_tag(self, tag_name, out_q):
        # TODO(asmacdo) temporary, use all tags. need to add whitelist (sync.py#223)
        tag_url = self.get_tag_url(tag_name)
        # tag_log.info("Retriving tag from: {url}".format(url=tag_url))
        tag_downloader = self.remote.get_downloader(tag_url)
        # Accept headers indicate the highest version the client (us) can use.
        # The registry will return Manifests of this and lower type.
        # TODO(asmacdo) make this a constant?
        await tag_downloader.run(extra_data=self.extra_request_data)
        data_type = tag_downloader.response_headers['Content-Type']

        manifest_list = manifest = None
        if data_type == MEDIA_TYPE.MANIFEST_LIST:
            manifest_list = await self.process_manifest_list(tag_downloader, out_q)
        elif data_type == MEDIA_TYPE.MANIFEST_V2:
            # skipped_content_types.add(MEDIA_TYPE.MANIFEST_V2)
            manifest = await self.process_manifest(tag_downloader, out_q)
        else:
            self._log_skipped_types.add(data_type)
        tag = Tag(name=tag_name, manifest=manifest, manifest_list=manifest_list)
        tag_log.info("OUT: new tag")
        tag_dc = DeclarativeContent(content=tag)
        await out_q.put(tag_dc)
        return tag_name

    async def process_manifest_list(self, tag_downloader, out_q):
        with open(tag_downloader.path, 'rb') as manifest_file:
            raw = manifest_file.read()
        digests = {k: v for k, v in tag_downloader.artifact_attributes.items()
                   if k in Artifact.DIGEST_FIELDS}
        size = tag_downloader.artifact_attributes['size']
        manifest_list_data = json.loads(raw)
        manifest_list_artifact = Artifact(
            size=size,
            file=tag_downloader.path,
            **digests
        )
        da = DeclarativeArtifact(
            artifact=manifest_list_artifact,
            url=tag_downloader.url,
            relative_path=tag_downloader.path,
            remote=self.remote,
            extra_data=self.extra_request_data,
        )
        manifest_list = ManifestList(
            digest=digests['sha256'],
            schema_version=manifest_list_data['schemaVersion'],
            media_type=manifest_list_data['mediaType'],
            # artifacts=[manifest_list_artifact] TODO(asmacdo) does this get set?
        )
        try:
            manifest_list_artifact.save()
        except Exception as e:
            manifest_list_artifact = Artifact.objects.get(sha256=digests['sha256'])
        try:
            manifest_list.save()
        except Exception as e:
            list_log.info("Already created, using existing copy")
            manifest_list = ManifestList.objects.get(digest=manifest_list.digest)

        list_log.info("OUT: new list")
        list_dc = DeclarativeContent(content=manifest_list, d_artifacts=[da])
        await out_q.put(list_dc)
        # TODO(asmacdo)
        for manifest in manifest_list_data.get('manifests', []):
            downloaded = await self.download_manifest(manifest['digest'])
            await self.process_manifest(downloaded, out_q)

    async def download_manifest(self, reference):
        manifest_url = self.get_tag_url(reference)
        # man_log.info("Retriving manifest from: {url}".format(url=manifest_url))
        downloader = self.remote.get_downloader(manifest_url)
        # Accept headers indicate the highest version the client (us) can use.
        # The registry will return Manifests of this and lower type.
        # TODO(asmacdo) make this a constant?
        await downloader.run(extra_data=self.extra_request_data)
        return downloader

    async def process_manifest(self, downloader, out_q):
        with open(downloader.path, 'rb') as manifest_file:
            raw = manifest_file.read()
        digests = {k: v for k, v in downloader.artifact_attributes.items()
                   if k in Artifact.DIGEST_FIELDS}
        size = downloader.artifact_attributes['size']
        manifest_data = json.loads(raw)
        manifest_artifact = Artifact(
            size=size,
            file=downloader.path,
            **digests
        )
        da = DeclarativeArtifact(
            artifact=manifest_artifact,
            url=downloader.url,
            relative_path=downloader.path,
            remote=self.remote,
            extra_data=self.extra_request_data,
        )
        manifest = ImageManifest(
            digest=digests['sha256'],
            schema_version=manifest_data['schemaVersion'],
            media_type=manifest_data['mediaType'],
            # artifacts=[manifest_artifact] TODO(asmacdo) does this get set?
        )
        try:
            manifest_artifact.save()
        except Exception as e:
            manifest_artifact = Artifact.objects.get(sha256=digests['sha256'])
        try:
            manifest.save()
        except Exception as e:
            # man_log.info("using existing manifest")
            manifest = ImageManifest.objects.get(digest=manifest.digest)

        # man_log.info("Successful creation.")
        man_dc = DeclarativeContent(content=manifest, d_artifacts=[da])
        man_log.info("OUT new manifest list")
        await out_q.put(man_dc)
        # TODO(asmacdo) is [] default mutable?
        for layer in manifest_data.get('layers', []):
            await self.process_blob(layer, manifest, out_q)

    async def process_blob(self, layer, manifest, out_q):
        sha256 = layer['digest'][len('sha256:'):]
        blob_artifact = Artifact(
            size=layer['size'],
            sha256=sha256
            # Size not set, its not downloaded yet
        )
        blob = ManifestBlob(
            digest=sha256,
            media_type=layer['mediaType'],
            manifest=manifest
        )
        da = DeclarativeArtifact(
            artifact=blob_artifact,
            # Url should include 'sha256:'
            url=self.layer_url(layer['digest']),
            # TODO(asmacdo) is this what we want?
            relative_path=layer['digest'],
            remote=self.remote,
            # extra_data="TODO(asmacdo)"
        )
        dc = DeclarativeContent(content=blob, d_artifacts=[da])
        blob_log.info("OUTPUT new blob")
        await out_q.put(dc)

    def get_tag_url(self, tag):
        relative_url = '/v2/{name}/manifests/{tag}'.format(
            name=self.remote.namespaced_upstream_name,
            tag=tag
        )
        return urljoin(self.remote.url, relative_url)

    @property
    def tags_list_url(self):
        relative_url = '/v2/{name}/tags/list'.format(name=self.remote.namespaced_upstream_name)
        return urljoin(self.remote.url, relative_url)

    def layer_url(self, digest):
        relative_url = '/v2/{name}/blobs/{digest}'.format(
            name=self.remote.namespaced_upstream_name,
            digest=digest
        )
        return urljoin(self.remote.url, relative_url)


class DockerDeclarativeVersion:

    def __init__(self, repository, pipeline, mirror=True):
        self.repository = repository
        self.pipeline = pipeline

    def create(self):
        """
        Perform the work. This is the long-blocking call where all syncing occurs.
        """
        with WorkingDirectory():
            with RepositoryVersion.create(self.repository) as new_version:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.pipeline)
