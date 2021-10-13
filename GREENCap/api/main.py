from redcap import Project
import shutil
import requests
import json
import sys
import os
import time
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
from testredcap import *
import multipart

app = FastAPI()

api_url = 'https://www.ctsiredcap.pitt.edu/redcap/api/'
api_key = 'B7E72510F0662853A0B0DA5A5EF574E4'
project = Project(api_url, api_key)

@app.get("/forms")
def forms():
    forms = project.forms
    return forms

@app.get("/fields")
def fields():
    fields = project.field_names
    return fields

# add parameter to request nums names, or both:
# Done
@app.get("/arms")
@app.get("/arms/{type}")
def arms(type: str = "names"):
    # nums, names, or both
    arm_names = project.arm_names
    arm_nums = project.arm_nums
    if type == "names":
        return arm_names
    if type == "nums":
        return arm_nums
    # default if someone enters something other than names and nums
    return arm_names

# return only unique event names
# Done
@app.get("/events")
def events():
    events = project.events
    events = [x['unique_event_name'] for x in events]
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
