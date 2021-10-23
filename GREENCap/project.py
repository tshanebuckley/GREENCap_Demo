# import dependencies
import atexit
import sys
import os
import yaml
import json
import redcap
import asyncio
import aiohttp
import pydantic
from datetime import date, datetime, time, timedelta
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

# TODO: create a Payload class that exists as a loosly verified list of payloads.
# Will become the self._payloads object in a Project.
# The payload should also include a reattempt method for retries of data pulls.
# Will also need a Tasks object

# redcap payload class
class REDCapPayload(pydantic.BaseModel):
    _id: str
    payloads: list = []
    response: str = None
    creation_time: datetime = datetime.now()
    request_time: datetime
    response_time: datetime
    call_time: timedelta
    status: str = 'created' # can be created, running, completed

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
# Would like plugins for MongoDB, NIDM, RDF, XNAT, etc
# REDCap instance would have a Project w/ access to the above resources
class Project:
    # use the original __init__ with sycn _call_api
    def __init__(self, projects=[], verify_ssl=True, lazy=False, 
                num_chunks=10, extended_by=['records'], 
                use_cfg=True, **kwargs):
        # initialize a url variable
        self.curr_url = ''
        # if set default to the yaml config settings
        if use_cfg:
            # try to load the yaml cfg
            try:
                self.cfg = utils.get_greencap_config()
            except:
                print("No config file found.")
            # initialize the base number of chunks for api calls
            try:
                self.num_chunks = self.cfg['num_chunks']
            except:
                self.num_chunks = num_chunks
                print("Using default number of chunks since no configuration was found.")
            # initialize the criteria to extend api calls by
            try:
                self.extended_by = self.cfg['extended_by']
            except:
                self.extended_by = extended_by
                print("Using default method to extend api calls by since no configuration was found.")
        # otherwise, just use the arguments given/set by default in code
        else:
            self.num_chunks = num_chunks
            self.extended_by = extended_by
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
        #self._responses = [] # responses are what is returned from the tasks

    # function to close the aiohttp client session on exit
    @atexit.register
    def _end_session(self):
        self._session.close()

    # overwrite the sync _call_api method
    def _call_api(self, payload, typpe, **kwargs): # async
        request_kwargs = self._kwargs
        request_kwargs.update(kwargs)
        rcr = redcap.RCRequest(self.curr_url, payload, typpe) # self.url, 
        return rcr, request_kwargs

    # method to get the list of id records of the defining field of a project
    def get_records(self, rc_name):
        # NOTE: simple implementation for now that should be made into a coroutine
        # instead of using base PyCap.
        # create a PyCap Projects
        #rc = redcap.Project(self.redcap[rc_name].url, self.redcap[rc_name].token)
        # run a selection to grab the list of records
        #record_list = utils.run_selection(project=rc, fields=self.redcap[rc_name].def_field, syncronous=True)
        record_list = utils.run_selection(project=rc_name, fields=rc_name.def_field, syncronous=True)
        # return the records
        return record_list

    # method to add a project
    # NOTE: plugins could be applied here by appending more data via add_project to the objects in
    # self.redcap or by extending the self.redcap dictionary itself (would want to rename this self.remotes
    # along with this function to add_remote and add a 'type' argument)
    def add_project(self, name):
        # try to add the project
        try:
            # use pydantic to create a verified redcap connection
            rc_data = create_redcap_project(name)
            # add the project to the dict
            self.redcap[rc_data.name] = redcap.Project(rc_data.url, rc_data.token, name=rc_data.name)
            # add a .records field that contains all of the values for the def_field
            self.redcap[rc_data.name].records = self.get_records(rc_name=self.redcap[rc_data.name])
            # run the alterations for _call_api
            setattr(self.redcap[rc_data.name], "_call_api", self._call_api)
        # log the failure
        except ValidationError as e:
            print(e.json())

    '''
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
    '''

    # gets a payload
    def get_payload(self, rc_name, func_name, **func_kwargs):
        # set the current url
        self.curr_url = self.redcap[rc_name].url
        # run the function
        rcr = eval("self.redcap['{name}'].".format(name=rc_name) + func_name + "(**func_kwargs)")
        # extract only the payload, a tuple of the url to make the request to and the payload
        return (self.redcap[rc_name].url, rcr.payload)

    # gets the payloads by extending to all possible calls and then chunking them
    def get_payloads(self, selection_criteria=None, extended_by=None, num_chunks=None, rc_name=None, func_name=None):
        # set some variables defined by the object if not set by the function
        if num_chunks == None:
            num_chunks = self.num_chunks
        if extended_by == None:
            extended_by = self.extended_by
        # get the api calls
        api_calls = utils.extend_api_calls(self.redcap[rc_name], selection_criteria=selection_criteria, extended_by=extended_by, num_chunks=num_chunks)
        # initialize a Payload object to save the payloads to
        pload = REDCapPayload()
        # for each api call
        for call in api_calls:
            # generate and save the payloads as a Payload object
            pload.add(self.get_payload(rc_name=rc_name, func_name=func_name, **call))
        # save this new Payload object within the class
        self._payloads.append(pload)

    # seems like the below 'add_tasks' function could be moved into Payloads object
    # along with a similar execute request function...though some type of execution
    # function would still be required in this class as a top-level call.
    def exec_request():
        pass

    # method to add get all of the tasks for a Payload object into a Tasks object
    def add_tasks(self, rc_name, func_name):
        tasks = []
        call_num = 0
        for api_call in api_calls:
            # iterate the call_num
            call_num = call_num + 1
            #print(api_call)
            task = asyncio.ensure_future()
            tasks.append(task)
        self._tasks.append(tasks)
