from pulpcore.plugin.models import Content, Repository, RepositoryVersion
from pulpcore.plugin.viewsets import NamedModelViewSet
from pulp_docker.app.models import (Manifest, MEDIA_TYPE, ManifestBlob, ManifestTag,
                                    BlobManifestBlob, ManifestListManifest)

import logging


log = logging.getLogger('********************')


def add_and_remove(repository_pk, add_content_units, remove_content_units, base_version_pk=None):
    """
    Create a new repository version by adding and then removing content units.

    Args:
        repository_pk (int): The primary key for a Repository for which a new Repository Version
            should be created.
        add_content_units (list): List of PKs for :class:`~pulpcore.app.models.Content` that
            should be added to the previous Repository Version for this Repository.
        remove_content_units (list): List of PKs for:class:`~pulpcore.app.models.Content` that
            should be removed from the previous Repository Version for this Repository.
        base_version_pk (int): the primary key for a RepositoryVersion whose content will be used
            as the initial set of content for our new RepositoryVersion
    """
    repository = Repository.objects.get(pk=repository_pk)

    if base_version_pk:
        base_version = RepositoryVersion.objects.get(pk=base_version_pk)
    else:
        base_version = None

    if '*' in remove_content_units:
        latest = RepositoryVersion.latest(repository)
        remove_content_units = latest.content.values_list('pk', flat=True)

    content_pks_to_add = recursive_content_to_add(add_content_units)
    print(content_pks_to_add)
    content_pks_to_remove = recursive_content_to_remove(remove_content_units)
    print(content_pks_to_remove)
    # with RepositoryVersion.create(repository, base_version=base_version) as new_version:
        # new_version.remove_content(Content.objects.filter(pk__in=content_pks_to_remove))
        # new_version.add_content(Content.objects.filter(pk__in=content_pks_to_add))


def recursive_content_to_add(add_content_units):
    manifest_lists_to_add = set()
    manifests_to_add = set()
    blobs_to_add = set()
    # new_version.add_content(Content.objects.filter(pk__in=add_content_units))
    for content_url in add_content_units:
        content = NamedModelViewSet.get_resource(content_url, Content).cast()
        if type(content) is Manifest:
            if content.media_type == MEDIA_TYPE.MANIFEST_LIST:
                manifest_lists_to_add.add(content)
            else:
                manifests_to_add.add(content)
        else:
            blobs_to_add.add(content)

    for manifest_list in manifest_lists_to_add:
        manifests_to_add.update(set(manifest_list.listed_manifests.all()))

    for manifest in manifests_to_add:
        blobs_to_add.update(manifest.blobs.all())
        if manifest.config_blob:
            blobs_to_add.add(manifest.config_blob)

    all_to_add = list(manifest_lists_to_add) + list(manifests_to_add) + list(blobs_to_add)
    content_pks_to_add = []
    for content in all_to_add:
        content_pks_to_add.append(content.pk)
    return content_pks_to_add


def recursive_content_to_remove(remove_content_units):
    manifest_lists_to_remove = set()
    manifests_to_remove = set()
    blobs_to_remove = set()
    # new_version.add_content(Content.objects.filter(pk__in=add_content_units))
    for content_url in remove_content_units:
        content = NamedModelViewSet.get_resource(content_url, Content).cast()
        if type(content) is Manifest:
            if content.media_type == MEDIA_TYPE.MANIFEST_LIST:
                # import ipdb; ipdb.set_trace()
                manifest_lists_to_remove.add(content)
            else:
                manifests_to_remove.add(content)
        else:
            blobs_to_remove.add(content)

    manifests_to_maybe_remove = set()
    blobs_to_maybe_remove = set()

    for manifest_list in manifest_lists_to_remove:
        manifests_to_maybe_remove.update(set(manifest_list.listed_manifests.all()))

    for manifest in list(manifests_to_remove) + list(manifests_to_maybe_remove):
        blobs_to_maybe_remove.update(set(manifest.blobs.all()))
        if manifest.config_blob:
            blobs_to_maybe_remove.add(manifest.config_blob)

    remove_list = list(manifest_lists_to_remove) + list(manifests_to_remove) + list(blobs_to_remove)
    content_pks_to_remove = []
    for content in remove_list + list(blobs_to_maybe_remove) + list(manifests_to_maybe_remove):
        content_pks_to_remove.append(content.pk)

    print(content_pks_to_remove)

    # Get a distinct set of manifests referenced by 1) manifest lists in repo, 2) manifest lists in
    # content_pks_to_add
    # manifest_lists = Manifest.objects.filter(
    #     pk__in=content_pks_to_add,
    #     media_type='manifest-list',
    # )
    # manifest_lists += Manifest.objects.filter(
    #     pk__in=repository['previous_version'].content,
    #     pk__not__in=content_pks_to_remove,
    #     media_type=manifest-list
    # ),
    #
    # # Get a distinct set of manifest (not list) referenced by 1) manifests in repo, manifests in
    # # content_pks_to_add,
    # manifests = Manifest.objects.filter(
    #     pk__in=content_pks_to_add,
    #     media_type='manifest',
    # )
    # manifests += Manifest.objects.filter(
    #     pk__in=repository['previous_version'].content,
    #     pk__not__in=content_pks_to_remove,
    #     media_type=manifest
    # )
    # manifests += manifest_lists.listed_manifests.distinct()
    #
    # # get a distinct set of blobs referenced by 1)manifests in the repo, 2)manifests to add
    # blobs = manifests.blobs.distinct()
    # blobs += manifests.config_blob.distinct()
    #
    # # somehow remove all units that are not listed
    # repository_version.remove_content(pk__not__in=[blobs, manifests, manifest_lists])
    #

if __name__ == "__main__":
    repo_url = "/pulp/api/v3/repositories/6080111f-7f62-407f-b375-02468cb40170/"
    repository = NamedModelViewSet.get_resource(repo_url, Repository)
    add_content_units = []
    # add_content_units = [
    #     "/pulp/api/v3/content/docker/manifests/fbb1fda8-4a39-452b-8a23-26002d34cdee/"
    # ]
    remove_content_units = [
        "/pulp/api/v3/content/docker/manifests/fbb1fda8-4a39-452b-8a23-26002d34cdee/"
    ]
    # remove_content_units = []
    add_and_remove(repository.pk, add_content_units, remove_content_units)
