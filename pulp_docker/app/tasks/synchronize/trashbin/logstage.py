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
                await self.action(to_log)
                self.wait_q.get()
            else:
                prev_running = False
        log.info("END LOG STAGE")
        await out_q.put(None)

    async def action(self, item):
            if isinstance(item.content, Tag):
                await self.handle_tag(item)
            elif isinstance(item.content, ImageManifest):
                log.warn("TODO SKIPPING MANIFESTS")
            elif isinstance(item.content, ManifestList):
                log.warn("TODO SKIPPING MANIFEST LISTS")
            elif isinstance(item.content, ManifestBlob):
                log.warn("TODO SKIPPING BLOBS")

    async def handle_tag(self, tag_dc):
        log.info("IN_Q (TAG) {tag}".format(tag=str(tag_dc.content)))
        # Tag should only have 1 artifact, and it is temporary.
        da = tag_dc.d_artifacts[0]
        import ipdb; ipdb.set_trace()
        print(tag_dc)



