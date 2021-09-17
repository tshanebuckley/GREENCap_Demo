from typing import Optional
from asgiref.sync import async_to_sync, sync_to_async
from mergedeep import merge, Strategy
from functools import reduce
import configparser
import fnmatch
import json
import yaml
import numpy as np
import pandas as pd
import os
import sys
import threading
import asyncio
import redcap
import contextlib
import itertools
import requests

# covenience function for parsing selections (arm, event, field)
def parse_field_single_selection(selection_item = None, selection_str = None):
    # removes any strings into a separate list (should be surrounded by "" or '')
    selection_str = selection_str.replace("[", "").replace("]", "").split(",")
    # get the selectors that are strings
    str_names = fnmatch.filter(selection_str, "'*'") + fnmatch.filter(selection_str, '"*"')
    # remove the string selectors from the int-based selectors
    for str_name in str_names:
        selection_str.remove(str_name)
    # convert the integer selectors back into their own string
    int_selectors = '[' + ','.join(selection_str) + ']'
    # remove and ' or " from the string-based selectors
    str_names = [x.replace('"', '').replace("'", "") for x in str_names]
    # get a list of selected integers
    selected = eval("list(np.r_{selection})".format(selection=int_selectors))
    # convert these integers into strings
    selected = [str(x) for x in selected]
    # add the strings back to the selected list
    selected.extend(str_names)
    # add the selection item as a prefix
    selected = [selection_item + "_" + x for x in selected]
    #os.system("echo " + str(selected))
    # return the list of fields
    return selected

# convenience function to split the form name from the selection string
def split_form_and_str(full_str = None):
    # split via the start of the bracket
    brace_spit = full_str.split('[')
    # form name will be the first item
    form_name = brace_spit[0]
    # selection string will be the last element
    select_str = "[" + brace_spit[-1]
    # return these as a dict
    return {"form_name": form_name, "select_str": select_str}

# method that generates chunks
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    #for i in range(0, len(lst), n): -> by a chunk-size n
    #    yield lst[i:i + n]
    for i in range(0, n): # number of chunks
        yield lst[i::n]

# method that runs the deep merge over a chunk
def merge_chunk(chunk_as_list_of_dicts):
    # initialize the return dict as an empty dict
    chunk_as_dict = dict()
    # merge all of the dicts in the chunk
    merge(chunk_as_dict, *chunk_as_list_of_dicts, strategy=Strategy.TYPESAFE_ADDITIVE)
    # drop duplicated items in any lists
    for key in chunk_as_dict.keys():
        # if the value is a list
        if isinstance(chunk_as_dict[key], list):
            # drop duplicate items
            chunk_as_dict[key] = list(set(chunk_as_dict[key]))
    # return the combined dict
    return chunk_as_dict

# TODO: parallelize this
# method to create all individual api calls for a selection, follows an "opt-in" approach instead of PyCaps's "opt-out" approach on selection
def extend_api_calls(selection_criteria=None, extended_by=['records'], num_chunks=10): # , 'fields', 'forms'
    # drop any empty selection criteria
    selection_criteria = {key: selection_criteria[key] for key in selection_criteria.keys() if selection_criteria[key] != []}
    # get the set of criteria to not extend by
    not_extended_by = set(selection_criteria.keys()) - set(extended_by)
    # if not_extended_by is empty, then set it to None
    if len(not_extended_by) == 0:
        not_extended_by = None
    # get the criteria not being extended by while removing them from the selection_criteria
    not_extended_by = {key: selection_criteria.pop(key) for key in not_extended_by}
    # converts the dict into lists with tags identifying the criteria: from criteria: value to <criteria>_value
    criteria_list = [[key + '_' + item for item in selection_criteria[key]] for key in selection_criteria.keys()]
    # gets all permutations to get all individual calls
    extended_call_list_of_tuples = list(itertools.product(*criteria_list))
    # method to convert the resultant list of tubles into a list of dicts
    def crit_tuple_to_dict(this_tuple, extend_to_dicts=None):
        # get the list of key
        keys = {x.split('_')[0] for x in this_tuple}
        # initialize the dicts
        this_dict = {this_key: [] for this_key in keys}
        # fill the list of dicts
        for item in this_tuple:
            # get the key
            key = item.split('_')[0]
            # get the value
            value = item.replace(key + '_', '', 1)
            # add the value
            this_dict[key].append(value)
        # if there were fields the calls were not extended by
        if extend_to_dicts != None:
            this_dict.update(not_extended_by)
        # return the list of dicts
        return this_dict
    # convert the list of lists back into a list of dicts
    extended_call_list_of_dicts = [crit_tuple_to_dict(this_tuple=x, extend_to_dicts=not_extended_by) for x in extended_call_list_of_tuples]
    #print(extended_call_list_of_dicts)
    # method to re-combine the max-width jobs split into n chunks
    def condense_to_chunks(all_api_calls, num_chunks):
        # chunk the api_calls list
        chunked_calls_unmerged = list(chunks(lst=all_api_calls, n=num_chunks))
        # merge the chunks idividual calls
        chunked_calls_merged = [merge_chunk(x) for x in chunked_calls_unmerged]
        # return the api calls
        return chunked_calls_merged
    # chunk the calls
    final_call_list = condense_to_chunks(all_api_calls=extended_call_list_of_dicts, num_chunks=num_chunks)
    #print(final_call_list)
    # return the list of api requests
    return final_call_list

# method to retun if a project has events
def has_events(project):
    # if the project has no events, return False
    if len(project.events) == 0:
        return False
    # otherwise, return True
    else:
        return True

# method to retun if a project has arms
def has_arms(project):
    # if the project has no arms, return False
    if len(project.arm_nums) == 0:
        return False
    # otherwise, return True
    else:
        return True

# method to return if a project is longitudinal or not
def is_longitudinal(project):
    # if the project has events or arms, return True
    if has_events(project) or has_arms(project):
        return True
    # otherwise, return false
    else:
        return False

@sync_to_async
def async_pycap(project, function_name, call_args, call_id=None):
    # use eval to dynamically pass a function name from PyCap
    func_response = eval('project.' + function_name + '(**{call_args})'.format(call_args=call_args))
    # print to console that the current call has finished
    if call_id == None:
        print("API call for {func} has completed.".format(func=function_name))
    else:
        print("API call for {func} has completed, ID Number: {id_num}.".format(func=function_name, id_num=str(call_id)))
    #print(func_response)
    return func_response

# async method to run a list of api calls
async def run_pycap_requests(project, function_name, api_calls):
    print('Trying async {num_of_calls} call(s) ...'.format(num_of_calls=str(len(api_calls))))
    # get thge list of asynchronous api calls
    tasks = []
    call_num = 0
    for api_call in api_calls:
        # iterate the call_num
        call_num = call_num + 1
        #print(api_call)
        task = asyncio.ensure_future(async_pycap(project=project, function_name=function_name, call_args=api_call, call_id=call_num))
        tasks.append(task)
    # run and return the list of calls
    response = await asyncio.gather(*tasks)
    return response

# method to drop arms from a returned api call (dict)
def drop_arms(arms_list, df):
    # drop arms not in the selection
    df = [x for x in df if x["redcap_event_name"].split("_arm_")[-1] in arms_list]
    # return the updated dict
    return df

# method to drop events from a returned api call (dict)
def drop_events(events_list, df):
    # drop events not in the selection
    df = [x for x in df if x["redcap_event_name"].split("_arm_")[0] in events_list]
    # return the updated dict
    return df

# method to trim unwanted longitudinal data (arms and events given as comma-separated strings)
def trim_longitudial_project(df, arms="", events="", n_cpus=1):
    # TODO: parallelize this step
    # initialize the input_is_dict boolean to False
    input_is_dict = False
    # if the df is a single dict
    if isinstance(df, dict):
        # then wrap it in a list
        df = [df]
        # set the input_is_dict boolean to True
        input_is_dict = True
    # if arms are given for the selection
    if arms != "":
        # TODO: check the regex instead
        arms_list = arms.split(",")
        # drop arms not in the arms selection
        df = [drop_arms(arms_list=arms_list, df=x) for x in df]
    # if events are given for the selection
    if events != "":
        # TODO: check the regex instead
        events_list = events.split(",")
        # drop events not in the events selection
        df = [drop_events(events_list=events_list, df=x) for x in df]
    # if the list is of length 1 and the input was a dict, return to just a dict
    if len(df) == 1 and isinstance(df, list) and input_is_dict:
        df = df[0]
    # return the trimmed project data (dict)
    return df

# method to save a pipeline as a url
#def save_pipeline_url(name, url):

# method to store md5 checksum of the logging data, will be used for caching queries
#def update_log_md5(project, seconds_earlier):
'''
import requests
data = {
    'token': project.token,
    'content': 'log',
    'logtype': 'record',
    'user': '', # remove ?
    'record': '', # remove ?
    'beginTime': '2017-09-19 14:22', # get the current time and go back in seconds
    'endTime': '', # endTime will be current time, remove?
    'format': 'json',
    'returnFormat': 'json'
}
r = requests.post(project.url, data=data).json() # create md5 from this and store somewhere
#print('HTTP Status: ' + str(r.status_code))
#print(r.json())
'''

# TODO: implement caching, custom to rerun if internal REDCap logging updated
# TODO: add defensive coding for whitespace handling, type checking, and field/form/record existence
# covenience function for prunning a parsed selection
def run_selection(project = None, records: Optional[str] = "", arms: Optional[str] = "", events: Optional[str] = "", fields: Optional[str] = "", syncronous=False, num_chunks=50):
    chosen_fields = [] # project.def_field
    chosen_forms = []
    chosen_records = []
    if records != "":
        chosen_records = records.split(';')
    # if fields are given for the selection
    if fields != "":
        # add the optional selections
        selected_fields = fields.split(";")
        # for each field
        for selection in selected_fields:
            # determine if the selection is a single field, a selection of fields, or an entire form
            is_type = field_or_form(project=project, item=selection)
            # if it is whole form
            if is_type == "form":
                # add the form selection
                chosen_forms.append(selection)
            # if it is a single field
            elif is_type == "field":
                # add the single field selection
                chosen_fields.append(selection)
            # if it is a selection of fields
            elif is_type == "field_selection":
                # split the selection
                split_str = split_form_and_str(full_str = selection)
                # parse the selection of multiple fields
                parsed_fields = parse_field_single_selection(selection_item = split_str["form_name"], selection_str = split_str["select_str"])
                # add the fields
                chosen_fields.extend(parsed_fields)
    # initialize the df as an empty list
    df = []
    # if running an asynchronous call
    if syncronous == False:
        # if records, fields, or forms were not selected, "opt-in" for all of that criteria
        # NOTE: REDCap's API opts-in by default, but we must set these criteria manually to setup asynchronous calls
        if chosen_records == []:
            # get the records 
            chosen_records = run_selection(project=project, fields=project.def_field, syncronous=True)
        if chosen_fields == [] and chosen_forms ==[]:
            # get all of the fields
            chosen_fields = project.field_names
        # get the kwargs
        selection_criteria = {"records": chosen_records, "fields": chosen_fields, "forms": chosen_forms}
        # get all of the possible single item api calls: not implemented yet
        api_calls = extend_api_calls(selection_criteria=selection_criteria, num_chunks=num_chunks)
        # drop any empty api_calls
        api_calls = [x for x in api_calls if x != {}]
        #print(api_calls)
        # run the api calls asynchronously
        results = asyncio.run(run_pycap_requests(project=project, function_name='export_records', api_calls=api_calls))
        #print(results)
        #print(len(results))
        #print(type(results))
        #print(results[0])
        # rename to match downstream logic
        df = results
    # if running a single call
    elif syncronous == True:
        # pull data using PyCap, convert to a pandas dataframe: will eventually be deprecated by async records call
        df = project.export_records(records=chosen_records, fields=chosen_fields, forms=chosen_forms)
        # wrap into a list of length 1 to follow iterative logic that follow
        df = [df]
    # at this point, return the df if it is empty
    if df == [[]]:
        df = dict()
        json.dumps(df)
        df = json.loads(df)
        return df
    # TODO: reformat the below to handle the single return dict (as given), or run each return dict and then merge
    print("Finished getting requests, trimming and elongating if longitudinal...")
    # if the project is longitudinal
    if is_longitudinal(project):
        # trin the longitudinal study of unwanted data
        df = trim_longitudial_project(df=df, arms=arms, events=events)
    # if the df is a list with a single dictionary
    if isinstance(df, dict):
        # convert the dictionary to a dataframe
        df = pd.DataFrame.from_dict(df)
    # if the df is a list of dictionaries
    elif isinstance(df, list):
        # convert the list of dictionaries to a list of dataframes
        df = [pd.DataFrame.from_dict(x) for x in df]
        # if there are multiple dataframe in the list
        if len(df) > 1:
            # merge the dataframes TODO: parallelize this merge
            df = reduce(lambda x, y: pd.merge(x, y, how='outer', suffixes=(False, False)), df)
        # otherwise
        else:
            df = df[0]
    #print(df)
    # if the study is longitudinal
    if is_longitudinal(project):
        # reformat to a wide dataframe
        df = df.pivot(index = project.def_field, columns = "redcap_event_name") #chosen_fields[0]
        collapsed_cols = []
        for col in df.columns:
            collapsed_cols.append(col[0] + '#' + col[1]) # '#' used to separate field and event
        df.columns = collapsed_cols
    #print(df)
    # TODO: implement pipe running here (with its own cache?)
    # here, if the dataframe is empty and the only chosen field is the def_field (allows returning only records names)
    if df.empty and chosen_fields == [project.def_field]:
        # then set the df to a set of the records
        df = tuple(df.index)
        # convert the set into a json string
        df = json.dumps(df)
    # otherwise
    else:
        # convert back to json and return
        df = df.to_json() 
    df = json.loads(df)
    print(df)
    return df

# convenience function for getting the greencap config file data, TODO: configure this to integrate with a system
def get_greencap_config():
    file_path = '../config/greencap_config.yaml'
    # open the file
    f = open(file_path,)
    # load the yaml as a dict
    d = yaml.load(f, Loader=yaml.FullLoader)
    # return the dict
    return(d)

# convenience function for getting the config file data, TODO: configure this to integrate with a system
def get_project_config(project = None):
    file_path = '../config/projects/{proj}.json'.format(proj=project)
    # open the file
    f = open(file_path,)
    # load the json as a dict
    d = json.load(f)
    # return the dict
    return(d)

# convenience function for creating a project config file
def create_project_config(name = None, url = None, token = None, identifier = None):
    pass

# convenience function to see if the item is a field or form
def field_or_form(project = None, item = None):
    is_field = False
    is_form = False
    # check if field
    if item in project.field_names:
        is_field = True
    # check if form
    if item in project.forms:
        is_form = True
    # return options
    if is_field == is_form == False and '[' in item:
        # split by '[' and grab the first item
        field_name = item.split('[')[0]
        if field_name in project.forms:
            return "field_selection"
    elif is_field == is_form == False:
        return "neither"
    elif is_field == True and is_form == False:
        return "field"
    elif is_field == False and is_form == True:
        return "form"
    elif is_field == is_form == True:
        return "both"

# convenience function for getting the field from the column name
def get_field_cname(cname="", base=False) -> str:
    # field will be the first item separated by #
    field = cname.split("#")[0]
    # if getting the base, also expect a trunder: "___"
    if base == True and "___" in field:
        # set the field to the field as accessed on REDCap
        field = field.split("___")[0]
    # return the field
    return field

# convenience function for getting the event from the column name
def get_event_cname(cname="", include_arm=False) -> str:
    # event will be the last item separated by "_"
    event = cname.split("_")[-1]
    # if not including the arm number
    if include_arm == False:
        # removes the arm number, last item separated by "_"
        event = event.split("_")
        event.pop(-1)
        event = "_".join(event)
    # return the event
    return event

# convenience function for getting the arm from the column name
def get_arm_cname(cname=0) -> int:
    # arm number will be the last
    arm = cname.split("_")[-1]
    # cast to an integer
    arm = int(arm)
    # return the arm
    return arm