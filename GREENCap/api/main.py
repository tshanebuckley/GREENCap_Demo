# import dependencies
from typing import Optional
from fastapi import FastAPI
from asgiref.sync import async_to_sync, sync_to_async
import redcap
import pandas as pd
import fnmatch
import json
import os
import sys
# allow utils and pipes to be part of scope
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
# import utils
from utils import utils
import pipes

# initialize FastAPI
app = FastAPI()

# general data dictionary query -> TODO: Make this async
@app.get("/redcap/{project}/")
def read_data(project: str, records: Optional[str] = "", arms: Optional[str] = "", events: Optional[str] = "", fields: Optional[str] = "", pipes: Optional[str] = ""):
    # NOTE: If the query string is saved somewhere, then the same pull can be saved, and automated
    # use project name to get url and token from config file
    cfg = utils.get_project_config(project=project)
    # connect to redcap
    rc = redcap.Project(cfg['url'], cfg['token'])
    # run the selection
    csv = utils.run_selection(project=rc, records=records, arms=arms, events=events, fields=fields)
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

# get request to list the files

# get request for files

# post request for files (with an option to zip the content)