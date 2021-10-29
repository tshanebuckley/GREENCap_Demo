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
import tqdm
from uuid import uuid4
from datetime import date, datetime, time, timedelta
from requests import RequestException, Session
from typing import Optional, List
from asgiref.sync import sync_to_async, async_to_sync
#from .async_wrapper import for_all_methods_by_prefix
#from .requests import GCRequest
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

# class to manage payloads, simply has an _id for each payload
#class REDCapRequestManager():
#    def __init__(self):
#        self.requests = list()

# NOTE: can has a custom error for this class that runs if it fails to initialize
# Something like this...
# try:
#     REDCapPayload()
# except:
#     REDCapPayloadError() 

# function to run a REDCapRequest

# TODO: come back and make this verify with pydantic
# redcap payload class
class REDCapRequest(): # pydantic.BaseModel
    #_id: str
    #payloads: list
    #response: list
    #creation_time: datetime
    #request_time: datetime
    #response_time: datetime
    #call_time: datetime
    #status: str = 'created' # can be created, running, completed

    # NOTE: all logic should be called when this class is initialized
    # NOTE: look into why forms for all records are failing
    # Will get payloads as tasks, execute the request, and return the response
    def __init__(self, _id, payloads, sleep_time, **data):
        '''
        The argument of **data must include values for 'url' and 'method'. It can also included
        other arguments used by aiohttp.ClientSession().request().
        '''
        #super().__init__(_id, payloads, **data)
        # set the id
        self._id = _id
        # create a session for this request
        self.session = aiohttp.ClientSession()
        # set the payload list
        self.payloads = payloads
        # set the creation time
        self.creation_time = datetime.now()
        # save the sleep time used per execution of each task
        self.sleep_time = sleep_time

    async def run(self):
        # sub function to apply a sleep
        # TODO: use same logic as below to read streams in the same thread as the request
        async def fetch(sleep_time, my_coroutine):
            await(asyncio.sleep(sleep_time))
            r = await my_coroutine
            #print(r)
            return r
        # set the task list
        tasks = list()
        # for each payload given
        for pload in self.payloads:
            # create a task 
            # NOTE: need a method to convert payloads to resuests so that they can be added to the list of tasks
            task = asyncio.ensure_future(apply_sleep(self.sleep_time, self.session.request(**pload)))
            # append that task to the list of tasks
            tasks.append(task)
        # add a progress bar
        #prog = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        # set the request_time
        self.request_time = datetime.now()
        # set the status to 'running'
        self.status = 'running'
        # execute the request with progress bar
        #self.response = await asyncio.gather(*tasks)
        self.response = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        # set the response time
        self.response_time = datetime.now()
        # set the status to 'completed'
        self.status = 'completed'
        # close the session
        await self.session.close()
        # set the call_time as response_time - request_time
        self.call_time = self.response_time - self.request_time
        # convert times into strings and call time into seconds
        self.creation_time = str(self.creation_time)
        self.request_time = str(self.request_time)
        self.response_time = str(self.response_time)
        self.call_time = self.call_time.total_seconds()
        #print(self.response)
        # set a content list
        tasks = list()
        # extract the content from the response into a variable
        for resp in self.response:
            # if the resp yielded content
            if resp.content._size > 0:
                # create a task
                task = asyncio.ensure_future(resp.content.read())
            # append to the list
            tasks.append(task)
        # verify the returned content
        try:
            response_length =  [x._cache['headers']['Content-Length'] for x in self.response]
            if '0' not in response_length:
                # extract the response content
                #self.content = await asyncio.gather(*tasks)
                self.content = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
                # create the dataframe
                #self.data = utils.clean_content(df=self.content, )
            else:
                self.content = []
        # otherwise
        except:
            print("Request failed: likely requires smaller chunks.")
            self.content = []

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
        #self._session = aiohttp.ClientSession()
        # initialize a dictionary of redcap projects
        # NOTE: will probably change this to self.remotes
        self.redcap = dict()
        # get the greencap Project a redcap Project to base itself around
        for project in projects:
            self.add_project(project)
        # add a variable for the current list of payloads
        self._payloads = dict() # payload is the data for the post request

    # function to close the aiohttp client session on exit
    #@atexit.register
    #def _end_session(self):
    #    self._session.close()

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
    @sync_to_async
    def get_payload(self, rc_name, func_name, method, **func_kwargs):
        # set the current url
        self.curr_url = self.redcap[rc_name].url
        # run the function
        rcr = eval("self.redcap['{name}'].".format(name=rc_name) + func_name + "(**func_kwargs)")
        # create a dictionary of the kwargs
        request_args = {'url':self.redcap[rc_name].url, 'data':rcr.payload, 'method':method}
        # extract only the payload, a tuple of the url to make the request to and the payload
        return request_args

    # gets the payloads by extending to all possible calls and then chunking them
    async def exec_request(self, method, selection_criteria=None, extended_by=None, num_chunks=None, rc_name=None, func_name=None, sleep_time=0):
        # set some variables defined by the object if not set by the function
        if num_chunks == None:
            num_chunks = self.num_chunks
        if extended_by == None:
            extended_by = self.extended_by
        # get the api calls
        api_calls = utils.extend_api_calls(self.redcap[rc_name], selection_criteria=selection_criteria, extended_by=extended_by, num_chunks=num_chunks)
        # log number of calls
        print("Executing {n} requests...".format(n=str(len(api_calls))))
        # initialize a Payload object to save the payloads to
        ploads = list()
        # for each api call
        for call in api_calls:
            # create a payload
            pload = asyncio.ensure_future(self.get_payload(rc_name=rc_name, func_name=func_name, method=method, **call))
            # generate and save the payloads as a Payload object
            ploads.append(pload)
        # run the payload generation
        ploads = await asyncio.gather(*ploads)
        # get an id for the payload/request
        _id = str(uuid4())
        # save this new Payload object within the class
        self._payloads[_id] = ploads
        # determine if the project is longitudinal
        long = utils.is_longitudinal(self.redcap['bsocial'])
        # create the request
        req = REDCapRequest(_id=_id, payloads=ploads, longitudinal=long, arms=None, events=None, sleep_time=sleep_time)
        # submit the request
        await req.run() # response = 
        # log that the calls have finished
        print("{n} requests have finished.".format(n=str(len(req.content))))
        # drop the payload from the _payloads dict
        #del self._payloads[_id]
        # return the response
        return req
