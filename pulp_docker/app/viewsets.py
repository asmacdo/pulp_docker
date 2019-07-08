"""
Check `Plugin Writer's Guide`_ for more details.

.. _Plugin Writer's Guide:
    http://docs.pulpproject.org/en/3.0/nightly/plugins/plugin-writer/index.html
"""

from django.db import transaction
from drf_yasg.utils import swagger_auto_schema

from pulpcore.plugin.models import Content, Repository, RepositoryVersion
from pulpcore.plugin.serializers import (
    AsyncOperationResponseSerializer,
    RepositorySyncURLSerializer,
)
# TODO add to plugin api
from pulpcore.app.viewsets.repository import RepositoryVersionCreateSerializer
from pulpcore.plugin.tasking import enqueue_with_reservation
from pulpcore.plugin.viewsets import (
    BaseDistributionViewSet,
    ContentFilter,
    ContentViewSet,
    NamedModelViewSet,
    RemoteViewSet,
    OperationPostponedResponse,)
from rest_framework import viewsets as drf_viewsets
from rest_framework.decorators import detail_route

from . import models, serializers, tasks


class ManifestTagFilter(ContentFilter):
    """
    FilterSet for Tags.
    """

    class Meta:
        model = models.ManifestTag
        fields = [
            'name',
        ]


class ManifestTagViewSet(ContentViewSet):
    """
    ViewSet for ManifestTag.
    """

    endpoint_name = 'manifest-tags'
    queryset = models.ManifestTag.objects.all()
    serializer_class = serializers.ManifestTagSerializer
    filterset_class = ManifestTagFilter

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestTag from a request.
        """
        raise NotImplementedError()


class ManifestViewSet(ContentViewSet):
    """
    ViewSet for Manifest.
    """

    endpoint_name = 'manifests'
    queryset = models.Manifest.objects.all()
    serializer_class = serializers.ManifestSerializer

    @transaction.atomic
    def create(self, request):
        """
        Create a new Manifest from a request.
        """
        raise NotImplementedError()


class BlobFilter(ContentFilter):
    """
    FilterSet for Blobs.
    """

    class Meta:
        model = models.ManifestBlob
        fields = [
            'digest',
        ]


class BlobViewSet(ContentViewSet):
    """
    ViewSet for ManifestBlobs.
    """

    endpoint_name = 'blobs'
    queryset = models.ManifestBlob.objects.all()
    serializer_class = serializers.BlobSerializer
    filterset_class = BlobFilter

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestBlob from a request.
        """
        raise NotImplementedError()


class DockerRemoteViewSet(RemoteViewSet):
    """
    A ViewSet for DockerRemote.
    """

    endpoint_name = 'docker'
    queryset = models.DockerRemote.objects.all()
    serializer_class = serializers.DockerRemoteSerializer

    # This decorator is necessary since a sync operation is asyncrounous and returns
    # the id and href of the sync task.
    @swagger_auto_schema(
        operation_description="Trigger an asynchronous task to sync content",
        responses={202: AsyncOperationResponseSerializer}
    )
    @detail_route(methods=('post',), serializer_class=RepositorySyncURLSerializer)
    def sync(self, request, pk):
        """
        Synchronizes a repository. The ``repository`` field has to be provided.
        """
        remote = self.get_object()
        serializer = RepositorySyncURLSerializer(data=request.data, context={'request': request})

        # Validate synchronously to return 400 errors.
        serializer.is_valid(raise_exception=True)
        repository = serializer.validated_data.get('repository')
        result = enqueue_with_reservation(
            tasks.synchronize,
            [repository, remote],
            kwargs={
                'remote_pk': remote.pk,
                'repository_pk': repository.pk
            }
        )
        return OperationPostponedResponse(result, request)


class DockerDistributionViewSet(BaseDistributionViewSet):
    """
    ViewSet for DockerDistribution model.
    """

    endpoint_name = 'docker'
    queryset = models.DockerDistribution.objects.all()
    serializer_class = serializers.DockerDistributionSerializer


class RecursiveAddRemove(drf_viewsets.ViewSet):
    """
    ViewSet for recursively adding and removing Docker content.

    Args:
    Optional:
        repository: repository to update
    """
    serializer_class = RepositoryVersionCreateSerializer
    # parser_classes = (MultiPartParser, FormParser)

    def create(self, request):
        """
        Queues a task that creates a new RepositoryVersion by adding and removing content units
        """
        # TODO repository needs to be required by serializer
        repository = NamedModelViewSet.get_resource(
            request.data.pop('repository'),
            Repository,
        )
        add_content_units = []
        remove_content_units = []
        # repository = self.get_parent_object()
        serializer = RepositoryVersionCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        if 'base_version' in request.data:
            base_version_pk = NamedModelViewSet.get_resource(
                request.data['base_version'],
                RepositoryVersion
            ).pk
        else:
            base_version_pk = None

        if 'add_content_units' in request.data:
            for url in request.data['add_content_units']:
                content = NamedModelViewSet.get_resource(url, Content)
                add_content_units.append(content.pk)

        if 'remove_content_units' in request.data:
            for url in request.data['remove_content_units']:
                if url == '*':
                    remove_content_units.append(url)
                else:
                    content = NamedModelViewSet.get_resource(url, Content)
                    remove_content_units.append(content.pk)

        result = enqueue_with_reservation(
            tasks.add_and_remove, [repository],
            kwargs={
                'repository_pk': repository.pk,
                'base_version_pk': base_version_pk,
                'add_content_units': add_content_units,
                'remove_content_units': remove_content_units
            }
        )
        return OperationPostponedResponse(result, request)
    #
    #
    # @swagger_auto_schema(
    #     operation_description="Create an artifact and trigger an asynchronous"
    #                           "task to create RPM content from it, optionally"
    #                           "create new repository version.",
    #     operation_summary="Upload a package",
    #     operation_id="upload_rpm_package",
    #     request_body=OneShotUploadSerializer,
    #     responses={202: AsyncOperationResponseSerializer}
    # )
    # def create(self, request):
    #     """Upload an RPM package."""
    #     artifact = Artifact.init_and_validate(request.data['file'])
    #     filename = request.data['file'].name
    #
    #     if 'repository' in request.data:
    #         serializer = OneShotUploadSerializer(
    #             data=request.data, context={'request': request})
    #         serializer.is_valid(raise_exception=True)
    #         repository = serializer.validated_data['repository']
    #         repository_pk = repository.pk
    #     else:
    #         repository_pk = None
    #
    #     try:
    #         artifact.save()
    #     except IntegrityError:
    #         # if artifact already exists, let's use it
    #         artifact = Artifact.objects.get(sha256=artifact.sha256)
    #
    #     async_result = enqueue_with_reservation(
    #         tasks.one_shot_upload, [artifact],
    #         kwargs={
    #             'artifact_pk': artifact.pk,
    #             'filename': filename,
    #             'repository_pk': repository_pk,
    #         })
    #     return OperationPostponedResponse(async_result, request)
    #
    #
    #
