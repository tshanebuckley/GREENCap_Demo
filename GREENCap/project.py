# import dependencies
import sys
import os
import yaml
import json
import redcap
import asyncio
import aiohttp
import pydantic
from typing import Optional, List
from asgiref.sync import sync_to_async, async_to_sync
from .async_wrapper import for_all_methods_by_prefix
from .requests import GCRequest
from .utils import utils

# method to create a redcap project
def create_redcap_project(name=None):
    creds = utils.get_project_config(project = "bsocial")
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
    name: str
    url: str
    token: str

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
    def __init__(self, projects=[], verify_ssl=True, lazy=False):
        # initialize a dictionary of redcap projects
        self.redcap = dict()
        # get the greencap Project a redcap Project to base itself around
        for project in projects:
            self.add_project(project)
        # add a variable for the current list of requests
        self._requests = []
        # add a variable for the current list of payloads
        self._payloads = []

    # overwrite the sync _call_api method
    def _call_api(self, payload, typpe, **kwargs): # async
        request_kwargs = self._kwargs()
        request_kwargs.update(kwargs)
        rcr = redcap.RCRequest(self.url, payload, typpe)
        return rcr, request_kwargs

    # method to add a project
    def add_project(project, ):
        # add the project to the dict
        self.projects[project.name] = redcap.Project(project.url, project.token, name=project.name)
        # run the alterations
        

    # gets a payload
    def get_payload(self, func_name, **func_kwargs):
        # run the function
        rcr = eval("self.redcap." + func_name + "(**func_kwargs)")
        # extract only the payload
        return rcr.payload

    # method to get tasks from payloads
    def get_tasks(self, payload):
        pass