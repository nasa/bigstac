''' Common functions for dealing with AWS '''

import configparser
import os

def access_keys(credentials_file:str) -> tuple[str, str]:
    ''' Get access keys from AWS credentials file. '''
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(credentials_file))
    access_key = config.get('cmr-sit', 'aws_access_key_id')
    secret_access_key = config.get('cmr-sit', 'aws_secret_access_key')
    return access_key, secret_access_key
