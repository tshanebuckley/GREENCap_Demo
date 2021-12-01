import pydantic
from typing import Optional
import json

class REDCapCred(pydantic.BaseModel):
    '''
    This object is for validating the REDCap credentials for an
    individual PyCap Project object.
    '''
    pass

# todo add validations and error messages
class GREENCapCred(pydantic.BaseModel):
    '''
    This object is for validating the credentials given to a GREENCap
    class object when adding a new REDCap object to it's list of
    modified PyCap Project objects. 
    '''
    name: str
    url: str
    token: str
    local: boolean
    cli: boolean
    cred: Optional[REDCapCred]

    if cred:
        name = cred.name
        url = cred.url
        local = cred.local
        cli = cred.cli


    @pydantic.root_validator(pre=True)
    @classmethod
    def check_connection(cls, value):
        try:
            redcap.Project(value["url"], value["token"])
        except:
            raise REDCapConnectError(name=values["name"],
            message="Unable to connect to REDCap project {name}.".format(name=values["name"]))
        return value

    @pydantic.root_validator(pre=True)
    @classmethod
    def check_name(cls, value):
        if value["name"] == None and value["cli"] == False:
            raise InvalidNameError(message="Name must be provided.")
