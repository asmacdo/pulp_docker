import asyncio

from .confluence_pipeline import create_sync_pipeline


# named to easily replace `synchronize` task by altering the imports.
def synchronize(remote_pk=None, repository_pk=None):
    """
    All we are going to do is n
    """
    pipeline = create_sync_pipeline()
    start_that_thing(pipeline)


def start_that_thing(pipeline):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pipeline)
