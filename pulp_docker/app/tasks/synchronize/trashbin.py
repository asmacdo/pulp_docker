
# class ManifestDownloadStage(Stage):
#
#     def __init__(self, remote):
#         # TODO make this a constant
#         self.extra_request_data = {'headers': V2_ACCEPT_HEADERS}
#         self.remote = remote
#
#     async def __call__(self, inc_manifest_in, manifest_out, blob_out):
#         while True:
#             manifest = await inc_manifest_in.get()
#             if manifest is None:
#                 break
#             downloaded = await self.download_manifest(manifest)
#             await self.process_manifest(downloaded, manifest_out, blob_out)
#         await manifest_out.put(None)
#         await blob_out.put(None)
#
#     async def download_manifest(self, manifest):
#         return "TODO"
#         # downloader = self.remote.get_downloader(V2_API.get_tag_url(self.remote, tag_name))
#         # await tag_downloader.run(extra_data=self.extra_request_data)
#         # data_type = tag_downloader.response_headers['Content-Type']
#         #
#         # # manifest_list = manifest = None
#         # if data_type == MEDIA_TYPE.MANIFEST_LIST:
#         #     manifest_list = await self.process_manifest_list(tag_downloader, manifest_list_out,
#         #                                                      inc_manifest_out)
#         #     await self.process_tag(tag_name, manifest_list, tag_out)
#         # elif data_type == MEDIA_TYPE.MANIFEST_V2:
#         #     # skipped_content_types.add(MEDIA_TYPE.MANIFEST_V2)
#         #     # await tag_out.put("Tag({name}): Manifest".format(name=tag_name))
#         #     # await manifest_list_out.put("Manifest: TAG({name})".format(name=tag_name))
#         #     manifest = await self.process_manifest(tag_downloader, manifest_out, blob_out)
#         #     await self.process_tag(tag_name, manifest, tag_out)
#
#     # TODO dedup with tag dl stage
#     async def process_manifest(self, downloader, manifest_out, blob_out):
#         await manifest_out.put("Manifest")
#         await blob_out.put("blob")
#         return "Manifest DC"

        # else:
        #     self._log_skipped_types.add(data_type)
        # await tag_out

        # tag = Tag(name=tag_name, manifest=manifest, manifest_list=manifest_list)
        # tag_log.info("OUT: new tag")
        # tag_dc = DeclarativeContent(content=tag)
        # await out_q.put(tag_dc)
        # return tag_name
#
#     async def process_manifest_list(self, tag_downloader, out_q):
#         with open(tag_downloader.path, 'rb') as manifest_file:
#             raw = manifest_file.read()
#         digests = {k: v for k, v in tag_downloader.artifact_attributes.items()
#                    if k in Artifact.DIGEST_FIELDS}
#         size = tag_downloader.artifact_attributes['size']
#         manifest_list_data = json.loads(raw)
#         manifest_list_artifact = Artifact(
#             size=size,
#             file=tag_downloader.path,
#             **digests
#         )
#         da = DeclarativeArtifact(
#             artifact=manifest_list_artifact,
#             url=tag_downloader.url,
#             relative_path=tag_downloader.path,
#             remote=self.remote,
#             extra_data=self.extra_request_data,
#         )
#         manifest_list = ManifestList(
#             digest=digests['sha256'],
#             schema_version=manifest_list_data['schemaVersion'],
#             media_type=manifest_list_data['mediaType'],
#             # artifacts=[manifest_list_artifact] TODO(asmacdo) does this get set?
#         )
#         try:
#             manifest_list_artifact.save()
#         except Exception as e:
#             manifest_list_artifact = Artifact.objects.get(sha256=digests['sha256'])
#         try:
#             manifest_list.save()
#         except Exception as e:
#             list_log.info("Already created, using existing copy")
#             manifest_list = ManifestList.objects.get(digest=manifest_list.digest)
#
#         list_log.info("OUT: new list")
#         list_dc = DeclarativeContent(content=manifest_list, d_artifacts=[da])
#         await out_q.put(list_dc)
#         # TODO(asmacdo)
#         for manifest in manifest_list_data.get('manifests', []):
#             downloaded = await self.download_manifest(manifest['digest'])
#             await self.process_manifest(downloaded, out_q)
#
#     async def download_manifest(self, reference):
#         manifest_url = self.get_tag_url(reference)
#         # man_log.info("Retriving manifest from: {url}".format(url=manifest_url))
#         downloader = self.remote.get_downloader(manifest_url)
#         # Accept headers indicate the highest version the client (us) can use.
#         # The registry will return Manifests of this and lower type.
#         # TODO(asmacdo) make this a constant?
#         await downloader.run(extra_data=self.extra_request_data)
#         return downloader
#
#     async def process_manifest(self, downloader, out_q):
#         with open(downloader.path, 'rb') as manifest_file:
#             raw = manifest_file.read()
#         digests = {k: v for k, v in downloader.artifact_attributes.items()
#                    if k in Artifact.DIGEST_FIELDS}
#         size = downloader.artifact_attributes['size']
#         manifest_data = json.loads(raw)
#         manifest_artifact = Artifact(
#             size=size,
#             file=downloader.path,
#             **digests
#         )
#         da = DeclarativeArtifact(
#             artifact=manifest_artifact,
#             url=downloader.url,
#             relative_path=downloader.path,
#             remote=self.remote,
#             extra_data=self.extra_request_data,
#         )
#         manifest = ImageManifest(
#             digest=digests['sha256'],
#             schema_version=manifest_data['schemaVersion'],
#             media_type=manifest_data['mediaType'],
#             # artifacts=[manifest_artifact] TODO(asmacdo) does this get set?
#         )
#         try:
#             manifest_artifact.save()
#         except Exception as e:
#             manifest_artifact = Artifact.objects.get(sha256=digests['sha256'])
#         try:
#             manifest.save()
#         except Exception as e:
#             # man_log.info("using existing manifest")
#             manifest = ImageManifest.objects.get(digest=manifest.digest)
#
#         # man_log.info("Successful creation.")
#         man_dc = DeclarativeContent(content=manifest, d_artifacts=[da])
#         man_log.info("OUT new manifest list")
#         await out_q.put(man_dc)
#         # TODO(asmacdo) is [] default mutable?
#         for layer in manifest_data.get('layers', []):
#             await self.process_blob(layer, manifest, out_q)
#
#     async def process_blob(self, layer, manifest, out_q):
#         sha256 = layer['digest'][len('sha256:'):]
#         blob_artifact = Artifact(
#             size=layer['size'],
#             sha256=sha256
#             # Size not set, its not downloaded yet
#         )
#         blob = ManifestBlob(
#             digest=sha256,
#             media_type=layer['mediaType'],
#             manifest=manifest
#         )
#         da = DeclarativeArtifact(
#             artifact=blob_artifact,
#             # Url should include 'sha256:'
#             url=self.layer_url(layer['digest']),
#             # TODO(asmacdo) is this what we want?
#             relative_path=layer['digest'],
#             remote=self.remote,
#             # extra_data="TODO(asmacdo)"
#         )
#         dc = DeclarativeContent(content=blob, d_artifacts=[da])
#         blob_log.info("OUTPUT new blob")
#         await out_q.put(dc)
        # manifest_list = ManifestList(
        #     digest=digests['sha256'],
        #     schema_version=manifest_list_data['schemaVersion'],
        #     media_type=manifest_list_data['mediaType'],
        #     # artifacts=[manifest_list_artifact] TODO(asmacdo) does this get set?
        # )
        # try:
        #     manifest_list_artifact.save()
        # except Exception as e:
        #     manifest_list_artifact = Artifact.objects.get(sha256=digests['sha256'])
        # try:
        #     manifest_list.save()
        # except Exception as e:
        #     list_log.info("Already created, using existing copy")
        #     manifest_list = ManifestList.objects.get(digest=manifest_list.digest)
        #
        # list_dc = DeclarativeContent(content=manifest_list, d_artifacts=[da])


# class ArtifactDownloader(Stage):
#     def __init__(self, max_concurrent_content=200, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.max_concurrent_content = max_concurrent_content
#
#     async def __call__(self, in_q, out_q):
#         runner = ArtifactDownloaderRunner(in_q, out_q, self.max_concurrent_content)
#         await runner.run()
#
#
# class TagDownloadStage(Stage):
#     def __init__(self, remote, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.remote = remote
#
#     async def __call__(self, tag_dc_in, tag_out, inc_manifest_out, manifest_out,
#                        manifest_list_out, blob_out):
#         runner = TagDownloaderRunner(tag_dc_in, tag_out, inc_manifest_out, manifest_out,
#                                      manifest_list_out, blob_out, self.max_concurrent_content)
#         runner.run()
#
#
# class LEFTOVER:
#     async def leftovers(self, tag_dc_in, tag_out, inc_manifest_out, manifest_out,
#                         manifest_list_out, blob_out):
#         tag_dc = 'placeholder'
#         await self.sanity_process_tag(tag_dc, tag_out, inc_manifest_out,
#                                       manifest_out, manifest_list_out, blob_out)
#         await tag_out.put(None)
#         await inc_manifest_out.put(None)
#         await manifest_out.put(None)
#         await manifest_list_out.put(None)
#
#     async def sanity_process_tag(self, tag_dc, tag_out, inc_manifest_out, manifest_out,
#                                  manifest_list_out, blob_out):
#
#         import ipdb; ipdb.set_trace()
#         print(tag_dc)
#
#
# class DummyOut:
#     async def put(arg):
#         await None
#
#
# class TagDownloaderRunner(ArtifactDownloaderRunner):
#     def __init__(self, tag_dc_in, tag_out, inc_manifest_out, manifest_out, manifest_list_out,
#                  blob_out, max_concurrent_content):
#         self.in_q = tag_dc_in
#         self.tag_out = self.tag_out
#         # TODO can we remove this?
#         self.out_q = DummyOut()
#         self.max_concurrent_content = max_concurrent_content
#
#     async def _handle_content_unit(self, content):
#         # HANDLE TAG
#         downloaders_for_content = self._downloaders_for_content(content)
#         if downloaders_for_content:
#             downloads = await asyncio.gather(*downloaders_for_content)
#             for download_result in downloads:
#                 # TODO should only run one time for tags
#                 manifest = self.process_manifest(content, download_result)
#             self.process_tag(content, manifest)
#         # no output
#         # await self.out_q.put(content)
#         return len(downloaders_for_content)
#
#     async def process_tag(self, tag, manifest):
#         """
#         Args:
#             manifest (TODO): Manifest or Manifest List
#         """
#         await self.tag_out.put("Tag({name}): {manifest}".format(name=tag.name, manifest=manifest))
#
#     async def process_manifest(self, content, download_result):
#         def url_lookup(x):
#             return x.url == download_result.url
#         d_artifact = list(filter(url_lookup, content.d_artifacts))[0]
#         manifest_artifact = d_artifact.artifact
#         if manifest_artifact.pk is None:
#             manifest_artifact = Artifact(
#                 **download_result.artifact_attributes,
#                 file=download_result.path
#             )
#             # d_artifact.artifact = manifest_artifact
#         da = DeclarativeArtifact(
#             artifact=manifest_artifact,
#             url=download_result.url,
#             relative_path=download_result.path,
#             remote=self.remote,
#             extra_data=self.extra_request_data,
#         )
#         with open(downloader.path, 'rb') as manifest_file:
#             raw = manifest_file.read()
#         manifest_data = json.loads(raw)
#         if manifest_data['mediaType'] == MEDIA_TYPE.MANIFEST_V2:
#             manifest = ImageManifest(
#                 digest=download_result['sha256'],
#                 schema_version=manifest_data['schemaVersion'],
#                 media_type=manifest_data['mediaType'],
#             )
#             man_dc = DeclarativeContent(content=manifest, d_artifacts=[da])
#             for layer in manifest_data.get('layers', []):
#                 await self.process_blob(layer, manifest, blob_out)
#             await self.manifest_out(man_dc)
#
#         elif manffest_data['mediaType'] == MEDIA_TYPE.MANIFEST_LIST:
#             manifest_list = ManifestList(
#                 digest=download_result['sha256'],
#                 schema_version=manifest_data['schemaVersion'],
#                 media_type=manifest_data['mediaType'],
#             )
#
#  async def process_manifest(self, downloader, out_q):
#         with open(downloader.path, 'rb') as manifest_file:
#             raw = manifest_file.read()
#         digests = {k: v for k, v in downloader.artifact_attributes.items()
#                    if k in Artifact.DIGEST_FIELDS}
#         size = downloader.artifact_attributes['size']
#         manifest_data = json.loads(raw)
#         manifest_artifact = Artifact(
#             size=size,
#             file=downloader.path,
#             **digests
#         )
#         da = DeclarativeArtifact(
#             artifact=manifest_artifact,
#             url=downloader.url,
#             relative_path=downloader.path,
#             remote=self.remote,
#             extra_data=self.extra_request_data,
#         )
#         manifest = ImageManifest(
#             digest=digests['sha256'],
#             schema_version=manifest_data['schemaVersion'],
#             media_type=manifest_data['mediaType'],
#             # artifacts=[manifest_artifact] TODO(asmacdo) does this get set?
#         )
#         try:
#             manifest_artifact.save()
#         except Exception as e:
#             manifest_artifact = Artifact.objects.get(sha256=digests['sha256'])
#         try:
#             manifest.save()
#         except Exception as e:
#             # man_log.info("using existing manifest")
#             manifest = ImageManifest.objects.get(digest=manifest.digest)
#
#         # man_log.info("Successful creation.")
#         man_dc = DeclarativeContent(content=manifest, d_artifacts=[da])
#         man_log.info("OUT new manifest list")
#         await out_q.put(man_dc)
#         # TODO(asmacdo) is [] default mutable?
#         for layer in manifest_data.get('layers', []):
#             await self.process_blob(layer, manifest, out_q)
#

                # artifacts=[manifest_artifact] TODO(asmacdo) does this get set?
            )
#         try:
#             manifest_artifact.save()
#         except Exception as e:
#             manifest_artifact = Artifact.objects.get(sha256=digests['sha256'])
#         try:
#             manifest.save()
#         except Exception as e:
#             # man_log.info("using existing manifest")
#             manifest = ImageManifest.objects.get(digest=manifest.digest)
#
#         # man_log.info("Successful creation.")
#         man_dc = DeclarativeContent(content=manifest, d_artifacts=[da])



    # async def run(self):
    #     self._pending = set()
    #     self._content_get_task = self._add_to_pending(self.in_q.get())
    #     with ProgressBar(message='Downloading Artifacts') as pb:
    #         try:
    #             while self._pending:
    #                 done, self._pending = await asyncio.wait(self._pending,
    #                                                          return_when=asyncio.FIRST_COMPLETED)
    #                 for task in done:
    #                     if task is self._content_get_task:
    #                         content = task.result()
    #                         if content is None:
    #                             self._content_get_task = None
    #                         else:
    #                             self._add_to_pending(self._handle_content_unit(content))
    #                     else:
    #                         download_count = task.result()
    #                         pb.done += download_count
    #                         pb.save()
    #                 if not self.shutdown:
    #                     if not self.saturated and self._content_get_task not in self._pending:
    #                         self._content_get_task = self._add_to_pending(self.in_q.get())
    #         except asyncio.CancelledError:
    #             for future in self._pending:
    #                 future.cancel()
    #             raise
    #     await self.out_q.put(None)




    # async def download_and_process_tag(self, tag_name, tag_out, inc_manifest_out, manifest_out,
    #                                    manifest_list_out, blob_out):
    #     # TODO(asmacdo) temporary, use all tags. need to add whitelist (sync.py#223)
    #     # tag_log.info("Retriving tag from: {url}".format(url=tag_url))
    #     tag_downloader = self.remote.get_downloader(V2_API.get_tag_url(self.remote, tag_name))
    #     await tag_downloader.run(extra_data=self.extra_request_data)
    #     data_type = tag_downloader.response_headers['Content-Type']
    #
    #     # manifest_list = manifest = None
    #     if data_type == MEDIA_TYPE.MANIFEST_LIST:
    #         manifest_list = await self.process_manifest_list(tag_downloader, manifest_list_out,
    #                                                          inc_manifest_out)
    #         await self.process_tag(tag_name, manifest_list, tag_out)
    #     elif data_type == MEDIA_TYPE.MANIFEST_V2:
    #         # skipped_content_types.add(MEDIA_TYPE.MANIFEST_V2)
    #         # await tag_out.put("Tag({name}): Manifest".format(name=tag_name))
    #         # await manifest_list_out.put("Manifest: TAG({name})".format(name=tag_name))
    #         manifest = await self.process_manifest(tag_downloader, manifest_out, blob_out)
    #         await self.process_tag(tag_name, manifest, tag_out)
    #
    # async def process_tag(self, tag_name, manifest, tag_out):
    #     """
    #     Args:
    #         manifest (TODO): Manifest or Manifest List
    #     """
    #     await tag_out.put("Tag({name}): {manifest}".format(name=tag_name, manifest=manifest))
    #
    # async def process_manifest_list(self, downloader, manifest_list_out, inc_manifest_out):
    #     await manifest_list_out.put("Manifest List")
    #     await inc_manifest_out.put("Undownloaded Manifest")
    #     return "Manifest List DC"
    #
    # async def process_manifest(self, downloader, manifest_out, blob_out):
    #     await manifest_out.put("Manifest")
    #     await blob_out.put("blob")
    #     return "Manifest DC"


class ArtifactDownloaderRunner():
    # PULP: Unchanged except docstring removal
    def __init__(self, in_q, out_q, max_concurrent_content):
        self.in_q = in_q
        self.out_q = out_q
        self.max_concurrent_content = max_concurrent_content

    @property
    def saturated(self):
        return len(self._pending) >= self.max_concurrent_content

    @property
    def shutdown(self):
        return self._content_get_task is None

    async def run(self):
        self._pending = set()
        self._content_get_task = self._add_to_pending(self.in_q.get())
        with ProgressBar(message='Downloading Artifacts') as pb:
            try:
                while self._pending:
                    done, self._pending = await asyncio.wait(self._pending,
                                                             return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        if task is self._content_get_task:
                            content = task.result()
                            if content is None:
                                self._content_get_task = None
                            else:
                                self._add_to_pending(self._handle_content_unit(content))
                        else:
                            download_count = task.result()
                            pb.done += download_count
                            pb.save()
                    if not self.shutdown:
                        if not self.saturated and self._content_get_task not in self._pending:
                            self._content_get_task = self._add_to_pending(self.in_q.get())
            except asyncio.CancelledError:
                for future in self._pending:
                    future.cancel()
                raise
        await self.out_q.put(None)

    def _add_to_pending(self, coro):
        task = asyncio.ensure_future(coro)
        self._pending.add(asyncio.ensure_future(task))
        return task

    async def _handle_content_unit(self, content):
        downloaders_for_content = self._downloaders_for_content(content)
        if downloaders_for_content:
            downloads = await asyncio.gather(*downloaders_for_content)
            self._update_content(content, downloads)
        await self.out_q.put(content)
        return len(downloaders_for_content)

    def _downloaders_for_content(self, content):
        downloaders_for_content = []
        for declarative_artifact in content.d_artifacts:
            if declarative_artifact.artifact.pk is None:
                expected_digests = {}
                validation_kwargs = {}
                for digest_name in declarative_artifact.artifact.DIGEST_FIELDS:
                    digest_value = getattr(declarative_artifact.artifact, digest_name)
                    if digest_value:
                        expected_digests[digest_name] = digest_value
                if expected_digests:
                    validation_kwargs['expected_digests'] = expected_digests
                if declarative_artifact.artifact.size:
                    expected_size = declarative_artifact.artifact.size
                    validation_kwargs['expected_size'] = expected_size
                downloader = declarative_artifact.remote.get_downloader(
                    declarative_artifact.url,
                    **validation_kwargs
                )
                downloaders_for_content.append(
                    downloader.run(extra_data=declarative_artifact.extra_data)
                )
        return downloaders_for_content

    def _update_content(self, content, downloads):
        for download_result in downloads:
            def url_lookup(x):
                return x.url == download_result.url
            d_artifact = list(filter(url_lookup, content.d_artifacts))[0]
            if d_artifact.artifact.pk is None:
                new_artifact = Artifact(
                    **download_result.artifact_attributes,
                    file=download_result.path
                )
                d_artifact.artifact = new_artifact


class ArtifactDownloader(Stage):
    def __init__(self, max_concurrent_content=200, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_concurrent_content = max_concurrent_content

    async def __call__(self, in_q, out_q):
        runner = ArtifactDownloaderRunner(in_q, out_q, self.max_concurrent_content)
        await runner.run()



