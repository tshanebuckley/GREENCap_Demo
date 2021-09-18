# import dependencies
import sys
import os
import yaml
import redcap
from asgiref.sync import sync_to_async, async_to_sync
from .async_wrapper import for_all_methods_by_name
# sync version -> from requests import RequestException, Session
from aiohttp import ClientSession
from aiohttp.web import HTTPException

RedcapError = HTTPException # sync version -> RequestException

_session = ClientSession() # sync version -> Session()

# inherits from PyCap's RCRequest Class
class GCRequest(redcap.RCRequest):
    # overwrite the sync execute method
    async def execute(self, **kwargs):
        """Execute the API request and return data
        Parameters
        ----------
        kwargs :
            passed to requests.Session.post()
        Returns
        -------
        response : list, str
            data object from JSON decoding process if format=='json',
            else return raw string (ie format=='csv'|'xml')
        """
        # sync version -> response = self.session.post()
        async with _session.post(self.url, data=self.payload, **kwargs) as response:
            response = await response.read()
            print(response)
            # Raise if we need to
            self.raise_for_status(response)
            content = self.get_content(response)
            return content, response.headers
