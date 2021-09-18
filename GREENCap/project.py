# import dependencies
import sys
import os
import yaml
import redcap
from asgiref.sync import sync_to_async, async_to_sync
from .async_wrapper import for_all_methods
from .request import GCRequest

# inherits from PyCap's Project Class
@for_all_methods_by_prefix(sync_to_async)
class Project(redcap.Project):
    # overwrite the sync _call_api method
    async def _call_api(self, payload, typpe, **kwargs):
        request_kwargs = self._kwargs()
        request_kwargs.update(kwargs)
        # make this async
        rcr = await GCRequest(self.url, payload, typpe)
        return rcr.execute(**request_kwargs)