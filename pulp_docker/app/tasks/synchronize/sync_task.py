from gettext import gettext as _
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository, RepositoryVersion  # noqa
from pulpcore.plugin.stages import (
    ArtifactDownloader,
    ArtifactSaver,
    ContentUnitAssociation,
    ContentUnitUnassociation,
    ContentUnitSaver,
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    EndStage,
)

# TODO(asmacdo) alphabetize
from pulp_docker.app.models import DockerRemote
# from pulp_docker.app.models ixmport (ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob,
# #                                     ManifestList, Tag)

from .docker_stages import InterrelateContent, ProcessContentStage, TagListStage, DidItWorkStage
from pulp_docker.app.tasks.stages.dedupe_save import StupidArtifactSave, StupidContentSave
from pulp_docker.app.tasks.stages.stage_group import StageGroup


log = logging.getLogger(__name__)


def synchronize(remote_pk, repository_pk):
    """
    TODO
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

    DockerDeclarativeVersion(repository, remote).create()


class DockerDeclarativeVersion(DeclarativeVersion):

    def __init__(self, repository, remote, mirror=True):
        self.repository = repository
        self.remote = remote
        self.mirror = mirror

    def pipeline_stages(self, new_version):
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
        content_layer = StageGroup([
            ArtifactDownloader(),
            StupidArtifactSave(),
            ProcessContentStage(self.remote),
            StupidContentSave(),
        ])
        handle_content = StageGroup([content_layer, content_layer, content_layer,
                                     InterrelateContent()])

        return [
            TagListStage(self.remote),
            handle_content,

            # TODO Third handle content skips save, due to blob model
            # blob model needs many-to-many with ImageManifest
            # ArtifactDownloader(),
            # StupidArtifactSave(),
            # ProcessContentStage(self.remote),
            # StupidContentSave(),
            DidItWorkStage(),
        ]
