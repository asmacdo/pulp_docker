from gettext import gettext as _
from urllib.parse import urljoin
import asyncio
import base64
import hashlib
import json
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository  # noqa
from pulpcore.plugin.stages import (
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    Stage
)

# TODO(asmacdo) alphabetize
from pulp_docker.app.models import ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob


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
    repository = Repository.objects.get(pk=repository_pk)
    first_stage = DockerFirstStage()
    wait_q = asyncio.Queue()
    log_stage = LogStage(wait_q)

    DockerDeclarativeVersion(first_stage, log_stage, repository).create()


class WaitOnMe:
    pass


class DockerFirstStage(Stage):
    async def __call__(self, in_q, out_q):
        log.info("First")
        for i in range(5):
            log.info("EMIT*******************: {i}".format(i=str(i)))
            await out_q.put(i)
            await asyncio.sleep(.5)
        await out_q.put(None)


class LogStage(Stage):
    def __init__(self, wait_q):
        self.wait_q = wait_q

    async def __call__(self, in_q, out_q):
        log.info("START LOG STAGE")
        prev_running = True
        while prev_running or not self.wait_q.empty():
            to_log = await in_q.get()
            if to_log is not None:
                self.wait_q.put(WaitOnMe)
                log.info("+++++++++++++++++LOG: {i}".format(i=str(to_log)))
                for i in range(to_log):
                    await asyncio.sleep(1)
                    in_q.put(i)
                self.wait_q.get()
            else:
                prev_running = False

        log.info("END LOG STAGE")
        await out_q.put(None)


class DockerDeclarativeVersion(DeclarativeVersion):

    def __init__(self, first_stage, log_stage, repository, mirror=None):
        self.first_stage = first_stage
        self.log_stage = log_stage
        self.repository = repository
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
            self.first_stage,
            self.log_stage,
        ]


