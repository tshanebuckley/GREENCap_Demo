# import dependencies
import requests
import pandas as pd
import io
import streamlit as st
import json
import os
import sys
import urllib # imported for urllib.parse.quote_plus("[1:2]") -> '%5B1%3A2%5D'
# import pydantic
# allow utils to be part of scope
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
# import utils
from utils import utils

# function to request data from REDCap via FastAPI
@st.cache
def GET_REDCap_data(project=None, arms = None, events = None, fields = None, pipes = None):
    # break out of the function if the project is not selected
    if project == None:
        return
    # load the project config file
    cfg = utils.get_project_config(project=project)
    # load the greencap config file
    gc = utils.get_greencap_config()
    # fields should be comma-separated fields, format for url injection
    url_fields = fields.replace(',', '%2C')
    # format the base url
    base_url = "http://{url}:{port}/redcap/{project}?fields={url_fields}".format(project=project, url_fields=url_fields)
    # run the get request and convert to a pandas dataframe
    urlData = requests.get(base_url).content
    rawData = pd.read_json(io.StringIO(urlData.decode('utf-8')))
    # set the row and column indexes?
    # return the dataframe
    return rawData

# main method definition
def main():
    page = st.sidebar.selectbox("Choose a page", ["REDCap Data Pull", "REDCap Exploration", "Add/Remove Project"])
    if page == "REDCap Data Pull":
        # selection objects
        project_selection = st.text_area('Select Project', '')
        arm_selection = st.text_area('Select Arms', '')
        event_selection = st.text_area('Select Events', '')
        field_selection = st.text_area('Select Fields', '')
        pipe_selection = st.text_area('Select Pipes', '')
        # get the redcap data

        #df = GET_REDCap_data(project="bsocial", identifier="registration_redcapid", fields="scan_date,scan_tasks")
        df = GET_REDCap_data(project=project_selection, identifier="registration_redcapid", fields="scan_date,scan_tasks")
        # if the resultant pandas dataframe is valide
        # display the df
        st.write(df)
        # otherwise
        # display the validation error
    elif page == "REDCap Exploration":
        pass
    elif page == "Add/Remove Project":
        pass

# main method
if __name__ == "__main__":
    main()