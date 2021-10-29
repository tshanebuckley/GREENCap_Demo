# import dependencies
from typing import Optional
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
from asgiref.sync import async_to_sync, sync_to_async
import redcap
import pandas as pd
import fnmatch
import json
import os
import sys
import shutil
import requests
import time
import multipart
# allow utils and pipes to be part of scope
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
# import utils
from utils import utils
import pipes
import greencap

# initialize a GREENCap object
grncap = greencap.Project()

# initialize FastAPI
app = FastAPI()

# general data dictionary query -> TODO: Make this async
@app.get("/redcap/{project}/")
def read_data(project: str, records: Optional[str] = "", arms: Optional[str] = "", events: Optional[str] = "", forms: Optional[str] = "", fields: Optional[str] = ""):
    # NOTE: If the query string is saved somewhere, then the same pull can be saved, and automated
    # use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    # run the selection
    csv = utils.run_selection(project=rc, records=records, arms=arms, events=events, forms=forms, fields=fields)
    # pipes should be run and potentially parallelized locally on the aggregated dataframe, perhaps utilize dagster here
    #csv = utils.run_pipes(df=csv, pipes=pipes)
    # return the resultant csv
    return csv
    
# pulls and formats a project's metadata, usually for building selectors and verifying choices
@app.get("/meta/{project}/{item}/")
def read_meta(project: str, item: str):
	# use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    # get the item 
    selected_item = eval("rc.{i}".format(i=item))
    #selected_item = json.loads(str(list(selected_item)))
    #os.system(selected_item)
    return selected_item

@app.get("/forms/{project}")
def forms(project: str):
    # use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    # return the forms
    return rc.forms

@app.get("/fields/{project}")
def fields(project: str):
    # use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    # return the fields
    return rc.field_names

# add parameter to request nums names, or both:
# Done
#@app.get("/arms/{project}")
@app.get("/arms/{project}/{type}")
def arms(project: str, type: str = "names"):
    # use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    # nums, names, or both
    if type == "names":
        return rc.arm_names
    elif type == "nums":
        return rc.arm_nums
    elif type == "dict":
        return dict(zip(rc.arm_names, rc.arm_nums))
    # default if someone enters something other than names and nums
    #return arm_names

# return only unique event names
# Done
@app.get("/events/{project}")
def events(project: str):
    # use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    events = rc.events
    events = [x['unique_event_name'].split('_arm_')[0] for x in events]
    return events

# add file functionality
@app.post("/file_upload/{record}/{field}/{unique_event_name}/{filename}")
async def file_upload(file: bytes = File(...),
                      record: str="",
                      field: str="",
                      unique_event_name: str=""):

    fname = filename + ".zip"
    with open(fname, "wb") as f:
        f.write(file)


    import_file(zip_file=fname,
                record=record,
                field=field,
                unique_event_name=unique_event_name,
                file_obj=None)

    os.remove(fname)



# look into aiohttp for asynchronous
