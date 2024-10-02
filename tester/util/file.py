'''Functions for managing files.'''

import os

# ######################################
#mark File Tools

def exists(path:str=None) -> bool:
    '''
    Tests if a file or directory exists
    Parameters:
        path (string): full path to file or directory to test for
    Returns:
        True if path exists, false otherwise
    '''
    path=os.path.realpath(__file__[:-2]+"txt") if path is None else os.path.expanduser(path)
    path=os.path.realpath(path) if not path.startswith("/") else path
    return os.path.exists(path)

def create(path:str=None):
    '''Create a directory using a path which can be local or absolute.'''
    path=os.path.realpath(path) if not path.startswith("/") else path
    os.makedirs(path, exist_ok=True)

def read(path:str=None):
    '''
    Read and return the contents of a file
    Parameters:
        path (string): full path to file to read
    Returns:
        None if file was not found, contents otherwise
    '''
    text = None
    path=os.path.realpath(__file__[:-2]+"txt") if path is None else os.path.expanduser(path)
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as file:
            text = file.read().strip()
            file.close()
    return text

def write(text:str, path:str=None):
    '''
    Write (creating if need be) file and set it's content
    Parameters:
        path (string): path to file to write
        text (string): content for file
    '''
    path=os.path.realpath(__file__[:-2]+"txt") if path is None else os.path.expanduser(path)
    with open(path, "w+", encoding="utf-8") as cache:
        cache.write(text)
        cache.close()

def delete(path:str=None):
    '''
    Delete file and set it's content
    Parameters:
        path (string): path to file to write
        text (string): content for file
    '''
    path=os.path.realpath(__file__[:-2]+"txt") if path is None else os.path.expanduser(path)
    os.remove(path)

def list_directory(path) -> list:
    ''' Return a list of files and directories contained by a given path. '''
    raw_list = os.listdir(path)
    item_list = []
    for i in raw_list:
        item_list.append(i)
    return item_list
