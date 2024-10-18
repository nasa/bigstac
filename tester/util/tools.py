import re
from datetime import datetime


def file_safe(raw:str) -> str:
    ''' Make sure no funny file names are created stick to what \w gives you or go home. '''
    return re.sub(r'[^\w]', "_", raw)

def iso_ish() -> str:
    '''
    Have a filename save version of an iso date and time so log files can be sorted and unique.
    '''
    current_datetime = datetime.now()
    file_name = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    return file_name
