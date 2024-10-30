''' Tools for dealing with the HTTP stats from DuckDB '''

import re
from util import aws

def parse_stat(name: str, text: str, prefix: str = '#') -> int:
    ''' Parse out the HTTP stats from a DuckDB call to S3 '''
    pattern = rf'{prefix}{name}:\s*(\d*\.?\d+)'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        raw = match.group(1)
        try:
            return int(raw)
        except ValueError:
            return float(raw)
    return 0

def parse_stat_unit(name: str, text: str) -> int:
    ''' Parse out the HTTP stats from a DuckDB call to S3 '''
    pattern = rf'{name}:\s*(\d*\.?\d+\s*\w*)'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        raw = match.group(1)
        parts = str(raw).strip().split()
        if len(parts) >= 2:
            value = float(parts[0])
            unit = parts[1]
            match unit:
                case 'TiB':
                    bytes_value = value * 1024 * 1024 * 1024 * 1024  # Convert TiB to bytes
                case 'GiB':
                    bytes_value = value * 1024 * 1024 * 1024  # Convert GiB to bytes
                case 'MiB':
                    bytes_value = value * 1024 * 1024  # Convert MiB to bytes
                case 'KiB':
                    bytes_value = value * 1024  # Convert KiB to bytes
                case _:
                    #some other unit, complain
                    bytes_value = -1
            return int(bytes_value)
        else:
            # no unit, assume bytes
            return int(raw)
    return 0

def parse_http_stats(raw_details: str) -> dict:
    ''' Parse out the HTTP Statistic section and return a dict of counts by HTTP verbs. '''
    pattern = r'HTTPFS(.*?)└' # ┘
    matches = re.findall(pattern, str(raw_details) , re.DOTALL)

    resp = {}
    for match in matches:
        resp['in'] = parse_stat_unit('in', match)
        resp['out'] = parse_stat_unit('out', match)
        for action in ['HEAD', 'GET', 'PUT', 'POST']:
            resp[action] = parse_stat(action, match)
        return resp
    return None

def create_secret(credential_file: str) -> str:
    key_id, access_key = aws.access_keys(credential_file)
    return f'''CREATE SECRET secret1(
        TYPE S3,
        KEY_ID '{key_id}',
        SECRET '{access_key}',
        REGION 'us-east-1');'''

# ################################################################################################ #
# In-line testing

sample = '''
HTTPFS \n HTTP Stats \n in: 10.2 MiB \n out: 0 \n #HEAD: 34 \n #GET: 12 \n #PUT: 0 \n #POST: 0 \n └
'''

expected = int(1024 * 1024 * 10.2)
actual = parse_stat_unit('in', sample)
assert expected == actual, f"In value: {expected} != {actual}"

expected = 0
actual = parse_stat_unit('out', sample)
assert expected == actual, f"Out value: {expected} != {actual}"

expected = {'in': 10695475, 'out': 0, 'HEAD': 34, 'GET': 12, 'PUT': 0, 'POST': 0}
actual = parse_http_stats(sample)
assert expected == actual, f"Basic read: {expected} != {actual}"
