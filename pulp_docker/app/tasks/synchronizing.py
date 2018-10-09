from gettext import gettext as _
from urllib.parse import urljoin
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository  # noqa
from pulpcore.plugin.stages import (
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    Stage
)

from pulp_docker.app.models import ImageManifest, DockerRemote


log = logging.getLogger(__name__)


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

    first_stage = DockerFirstStage(remote)
    DeclarativeVersion(first_stage, repository).create()


class DockerFirstStage(Stage):
    """
    The first stage of a pulp_docker sync pipeline.
    """

    def __init__(self, remote):
        """
        The first stage of a pulp_docker sync pipeline.

        Args:
            remote (FileRemote): The remote data to be used when syncing

        """
        self.remote = remote

    async def __call__(self, in_q, out_q):
        """
        Build and emit `DeclarativeContent` from the Manifest data.

        Args:
            in_q (asyncio.Queue): Unused because the first stage doesn't read from an input queue.
            out_q (asyncio.Queue): The out_q to send `DeclarativeContent` objects to

        """
        downloader = self.remote.get_downloader(self.tags_list_url)
        tags_result = await downloader.run()
        await out_q.put(None)

        # Use ProgressBar to report progress
        # for entry in self.read_my_metadata_file_somehow(result.path):
        #     unit = ImageManifest(entry)  # make the content unit in memory-only
        #     artifact = Artifact(entry)  # make Artifact in memory-only
        #     da = DeclarativeArtifact(artifact, entry.url, entry.relative_path, self.remote)
        #     dc = DeclarativeContent(content=unit, d_artifacts=[da])
        #     await out_q.put(dc)
        # await out_q.put(None)

    def read_my_metadata_file_somehow(self, path):
        """
        Parse the metadata for docker Content type.

        Args:
            path: Path to the metadata file
        """
        pass

    @property
    def tags_list_url(self):
        relative_url = '/v2/{name}/tags/list'.format(name=self.remote.namespaced_upstream_name)
        return urljoin(self.remote.url, relative_url)
