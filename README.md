## GREENCap

This repo exists as a test repository for application development for REDCap data.

Currently, this repo includes a Python SDK that wraps PyCap, but runs asynchronous REDCap requests, a FastAPI server that utilizes the asyncronous SDK, and an RShiny frontend.

Eventually, this repo will be archived and the respective sub-components of SDK, API, and Shiny App will be split into their own repos. 

An extensible and more production ready version of the API will also be made using FastAPI Nano.

## Goal of this repo

1. Recreate some of the functionality of REDCapR, but in native Python.
2. Successfully wrap PyCap to create api payloads, but run these asynchronously.
3. Create a REST-like API for working with data that abstracts the api token and url.
4. Test this REST-like API with a demo FastAPI service and RShiny FrontEnd.

## To Install

```bash
pip install --user git+https://github.com/tshanebuckley/GREENCap_Demo.git
```

## Basic usage

The goal is to keep the usage as close to PyCap as possible while extending the usage.

```python
# import the module
import greencap
# import asyncio
import asyncio
# create a "GREENCap Project"
gc = greencap.Project()
# add a project (saved under ~/.greencap/projects/my_project.json where "my_project" is the name of your REDCap Project)
# my_project.json is a simple json file that holds your url and api token
gc.add_project('bsocial')
# fecth your data asynchronously
r = asyncio.run(gc.exec_request(method='GET', selection_criteria={'fields': {'field_name'}}, rc_name='my_project', func_name='export_records'))
```

## Example project json

```json
{
  "url": "your url",
  "token": "your api token"
}
```
