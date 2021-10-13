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
import asyncio

RedcapError = HTTPException # sync version -> RequestException

#_session = ClientSession() # sync version -> Session()

# inherits from PyCap's RCRequest Class
class GCRequest(redcap.RCRequest):
    # use the original __init__ with sycn _call_api
    def __init__(self, *args, **kwargs):
        # use the parent class' __init__
        super(GCRequest, self).__init__(*args, **kwargs)
        #self._session = ClientSession()

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
        print("EXECUTE KWARGS")
        print(kwargs)
        #async def execute_task(url, data, **kwargs):
        async with ClientSession() as _session:
            async with _session.post(self.url, data=self.payload) as response: # , **kwargs
                print(self.payload)
                response = await response.read()
                print(response)
                # Raise if we need to
                self.raise_for_status(response)
                content = self.get_content(response)
                return content, response.headers
        # create a task
        #task = asyncio.create_task()
        #task = "task"
        # return the task
        #return task
