from gettext import gettext as _
from urllib.parse import urljoin
import asyncio
import json
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository, RepositoryVersion  # noqa
from pulpcore.plugin.stages import (
    ArtifactDownloader,
    ContentUnitAssociation,
    ContentUnitUnassociation,
    ContentUnitSaver,
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    EndStage,
    QueryExistingContentUnits,
    Stage,
)

from pulpcore.plugin.tasking import WorkingDirectory

# TODO(asmacdo) alphabetize
from pulp_docker.app.models import (ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob,
                                    ManifestList, Tag)

from .stages import (ConfluenceStage, ProcessNestedContentStage,
                     DidItWorkStage, StupidArtifactSave, StupidContentSave,
                     QueryAndSaveArtifacts, TagListStage)


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

    # TODO get create_pipe shouldn't need remote?
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
        return [
            TagListStage(self.remote),

            ArtifactDownloader(),
            StupidArtifactSave(),
            ProcessNestedContentStage(self.remote),
            StupidContentSave(),

            ArtifactDownloader(),
            StupidArtifactSave(),
            ProcessNestedContentStage(self.remote),
            StupidContentSave(),

            ArtifactDownloader(),
            StupidArtifactSave(),
            ProcessNestedContentStage(self.remote),
            StupidContentSave(),

            # QueryExistingContentUnits(),
            # TODO move this stage (and my patch) to docker
            # ContentUnitSaver(),
            #
            # ArtifactDownloader(),
            # QueryAndSaveArtifacts(),
            # ProcessNestedContentStage(self.remote),
            # QueryExistingContentUnits(),
            # ContentUnitSaver(),
            #
            # ArtifactDownloader(),
            # QueryAndSaveArtifacts(),
            # ProcessNestedContentStage(self.remote),
            # QueryExistingContentUnits(),
            # ContentUnitSaver(),
            # QueryExistingArtifacts(), ArtifactDownloader(), ArtifactSaver(),
            # QueryExistingContentUnits(), ContentUnitSaver(),
        ]


    # def create(self):
    #     """
    #     Perform the work. This is the long-blocking call where all syncing occurs.
    #     """
    #     with WorkingDirectory():
    #         with RepositoryVersion.create(self.repository) as new_version:
    #             pipeline = create_sync_pipeline(self.remote, new_version)
    #             loop = asyncio.get_event_loop()
    #             loop.run_until_complete(pipeline)
    #
