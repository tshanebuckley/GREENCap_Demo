# import dependencies
import sys
import os
import yaml
import json
import redcap
import asyncio
import aiohttp
import pydantic
from requests import RequestException, Session
from typing import Optional, List
from asgiref.sync import sync_to_async, async_to_sync
from .async_wrapper import for_all_methods_by_prefix
from .requests import GCRequest
from .utils import utils

# method to create a redcap project
def create_redcap_project(name=None):
    creds = utils.get_project_config(project = name)
    creds["name"] = name
    return REDCapProject(**creds)

# redcap connection error
class REDCapConnectError(Exception):
    def __init__(self, name:str, message:str) -> None:
        self.name = name
        self. message = message
        super().__init__(message)

# pydantic BaseClass object for a REDCap project
class REDCapProject(pydantic.BaseModel):
    url: str
    token: str
    name: str

    @pydantic.root_validator(pre=True)
    @classmethod
    def check_connection(cls, values):
        try: 
            redcap.Project(values["url"], values["token"])
        except:
            raise REDCapConnectError(name=values["name"], 
            message="Unable to connect to REDCap project {name}.".format(name=values["name"]))
        return values


# based off of PyCap's Project Class
# TODO: apply Plugin Design Pattern
class Project:
    # use the original __init__ with sycn _call_api
    def __init__(self, projects=[], verify_ssl=True, lazy=False, **kwargs):
        # initialize a url variable
        self.curr_url = ''
        # initialize kwargs
        self._kwargs = kwargs
        # initialize the aiohttp client
        self._session = aiohttp.ClientSession()
        # initialize a dictionary of redcap projects
        self.redcap = dict()
        # get the greencap Project a redcap Project to base itself around
        for project in projects:
            self.add_project(project)
        # add a variable for the current list of payloads
        self._payloads = [] # payload is the data for the post request
        # add a variable for the current list of requests
        self._requests = [] # requests are the payloads converted to requests
        # add a variable for the current list of requests
        self._tasks = [] # tasks are the requests converted into tasks
        # add a variable for the current list of responses
        self._responses = [] # responses are what is returned from the tasks

    # overwrite the sync _call_api method
    def _call_api(self, payload, typpe, **kwargs): # async
        request_kwargs = self._kwargs
        request_kwargs.update(kwargs)
        rcr = redcap.RCRequest(self.curr_url, payload, typpe) # self.url, 
        return rcr, request_kwargs

    # method to add a project
    def add_project(self, name):
        # try to add the project
        try:
            # use pydantic to create a verified redcap connection
            rc_data = create_redcap_project(name)
            # add the project to the dict
            self.redcap[rc_data.name] = redcap.Project(rc_data.url, rc_data.token, name=rc_data.name)
            # run the alterations
            setattr(self.redcap[rc_data.name], "_call_api", self._call_api)
        # log the failure
        except ValidationError as e:
            print(e.json())

    # add a request
    async def exec_request(self, data, method='POST', url=self.curr_url):
        try:
            response = await self._session.request(method=method, url=url, data=data)
            response.raise_for_status()
            print(f"Response status ({url}): {response.status}")
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"An error ocurred: {err}")
            response_json = await response.json()
            return response_json

    # execute a request
    def exec_requests(self):
        pass

    # gets a payload
    def get_payload(self, rc_name, func_name, **func_kwargs):
        # set the current url
        self.curr_url = self.redcap[rc_name].url
        # run the function
        rcr = eval("self.redcap['{name}'].".format(name=rc_name) + func_name + "(**func_kwargs)")
        # extract only the payload
        return rcr.payload

    # TODO: utilize the chunking from utils
    # gets the payloads by extending to all possible calls and then chunking them
    def get_payloads():
        pass

    # method to add tasks from payloads
    def add_task(self, rc_name, func_name):
        tasks = []
        call_num = 0
        for api_call in api_calls:
            # iterate the call_num
            call_num = call_num + 1
            #print(api_call)
            task = asyncio.ensure_future()
            tasks.append(task)
        self._tasks.append(tasks)
