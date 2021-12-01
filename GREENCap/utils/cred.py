import pydantic
from typing import Optional
import json
# todo add validations and error messages
class Cred(pydantic.BaseModel):

    name: str
    url: str
    token: str
    local: boolean
    cli: boolean
    cred: Optional[Cred]

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
