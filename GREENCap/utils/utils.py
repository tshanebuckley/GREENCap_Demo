from typing import Optional
from asgiref.sync import async_to_sync, sync_to_async
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

# method to create all individual api calls for a selection, follows an "opt-in" approach instead of PyCaps's "opt-out" approach on selection
# TODO: Update this method for selecting arguments to extend by and number of chunks
# apply chunking to extended_call_list_of_dicts -> can probably use deepmerge for this as well
def extend_api_calls(selection_criteria=None, extended_by=['records'], num_chunks=5): # , 'fields', 'forms'
    # drop any empty selection criteria
    selection_criteria = {key: selection_criteria[key] for key in selection_criteria.keys() if selection_criteria[key] != []}
    #print(selection_criteria)
    # get the set of criteria to not extend by
    not_extended_by = set(selection_criteria.keys()) - set(extended_by)
    #print(not_extended_by)
    # if not_extended_by is empty, then set it to None
    if len(not_extended_by) == 0:
        not_extended_by = None
    #print(not_extended_by)
    # get the criteria not being extended by while removing them from the selection_criteria
    not_extended_by = {key: selection_criteria.pop(key) for key in not_extended_by}
    #print(not_extended_by)
    # converts the dict into lists with tags identifying the criteria: from criteria: value to <criteria>_value
    criteria_list = [[key + '_' + item for item in selection_criteria[key]] for key in selection_criteria.keys()]
    #print(criteria_list)
    # gets all permutations to get all individual calls
    extended_call_list_of_tuples = list(itertools.product(*criteria_list))
    print("----------------------------------------------------")
    #print(extended_call_list_of_tuples)
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
        #print(this_dict)
        if extend_to_dicts != None:
            this_dict.update(not_extended_by)
        # return the list of dicts
        #print(dict_list)
        return this_dict
    # convert the list of lists back into a list of dicts
    extended_call_list_of_dicts = [crit_tuple_to_dict(this_tuple=x, extend_to_dicts=not_extended_by) for x in extended_call_list_of_tuples]
    #print(extended_call_list_of_dicts)
    # re-add the criteria that we do not extend out calls by
    #extended_call_list_of_dicts = [x = x.update(not_extended_by) for x in extended_call_list_of_dicts]
    print(extended_call_list_of_dicts)
    # if the call is being
    # return the list of api requests
    return None #extended_call_list_of_dicts

@sync_to_async
def async_pycap(project, function_name, call_args):
    func_response = eval('project.' + function_name + '(**{call_args})'.format(call_args=call_args))
    print(func_response)
    return func_response

# async method to run a list of api calls
async def run_pycap_requests(project, function_name, api_calls):
    print('Trying async {num_of_calls} call(s) ...'.format(num_of_calls=str(len(api_calls))))
    # get thge list of asynchronous api calls
    tasks = []
    for api_call in api_calls:
        print(api_call)
        task = asyncio.ensure_future(async_pycap(project=project, function_name=function_name, call_args=api_call))
        tasks.append(task)
    # run and return the list of calls
    response = await asyncio.gather(*tasks)
    return response

# covenience function for prunning a parsed selection
def run_selection(project = None, records: Optional[str] = "", arms: Optional[str] = "", events: Optional[str] = "", fields: Optional[str] = "", syncronous=False, num_chunks=5):
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
        #print(api_calls)
        #print(project.export_records(**api_calls[0]))
        # run the api calls asynchronously
        results = asyncio.run(run_pycap_requests(project=project, function_name='export_records', api_calls=api_calls))
        #results = asyncio.run(async_export(project=project, api_calls=api_calls))
        #print(results)
        # format the results as if they were a single call -> NOTE: Should be able to use mergedeep package here

    # if running a single call
    elif syncronous == True:
        # pull data using PyCap, convert to a pandas dataframe: will eventually be deprecated by async records call
        df = project.export_records(records=chosen_records, fields=chosen_fields, forms=chosen_forms)

    # at this point, return the df if it is empty
    if df == []:
        df = dict()
        json.dumps(df)
        df = json.loads(df)
        return df
    # TODO: consider non-longitudinal studies? Skip arms and events if so? Query if longitudinal using PyCap metadata?
    # if arms are given for the selection
    if arms != "":
        # TODO: check the regex instead
        arms_list = arms.split(",")
        # drop arms not in the selection
        df = [x for x in df if x["redcap_event_name"].split("_arm_")[-1] in arms_list]
	# if events are given for the selection
    if events != "":
        # TODO: check the regex instead
        events_list = events.split(",")
        # drop events not in the selection
        df = [x for x in df if x["redcap_event_name"].split("_arm_")[0] in events_list]
    # convert the dictionary to a dataframe
    df = pd.DataFrame.from_dict(df)
	# reformat to a wide dataframe
    df = df.pivot(index = project.def_field, columns = "redcap_event_name") #chosen_fields[0]
    collapsed_cols = []
    for col in df.columns:
        collapsed_cols.append(col[0] + '#' + col[1]) # '#' used to separate field and event
    df.columns = collapsed_cols
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