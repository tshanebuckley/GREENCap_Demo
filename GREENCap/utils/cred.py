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
