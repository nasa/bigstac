''' All functions related to the Stats object, an object to collect statistical information. '''

import statistics
import csv
import json

class Stats():
    ''' An object to track status in a dictionary with some convenience functions to manage them '''
    stats: dict

    def __init__(self):
        ''' Just getting things started with some defaults '''
        self.stats =  {}
        self.subs = {}

    def _ensure(self, name:str, init_value:int|float|list) -> bool:
        '''
        Internal function to ensure that a value exists and has a default value before using it.
        Return True if the init_value was used, meaning the stat did not exist; True means created.
        '''
        if not name in self.stats:
            self.stats[name] = init_value
            return True
        return False

    def get_sub(self, name:str) -> any:
        if not name in self.subs:
            self.subs[name] = Stats()
        return self.subs[name]

    def get(self, name:str, init_value:int|float) -> int|float:
        ''' Return a value stored, or use the initial value. '''
        self._ensure(name, init_value)
        return self.stats[name]

    def note(self, name:str, value:str):
        ''' Store a note in the stats overwriting any existing value. '''
        self.stats[name] = value

    def store(self, name:str, value:int|float):
        ''' Store a value in the stats overwriting any existing value. '''
        self.stats[name] = value

    def add(self, name:str, value:int|float):
        ''' Add a value to an existing stat. '''
        if not self._ensure(name, value):
            self.stats[name] = self.stats[name] + value

    def append(self, name:str, value:int|float):
        self._ensure(name, [])
        self.stats[name].append(value)

    def value(self, value:int|float, data:str = None):
        self.add('count', 1)
        self.add('total', value)
        self.min('min', value, {'min-id': data} if data else None)
        self.max('max', value, {'max-id': data} if data else None)
        self.append('list', value)
        #recalculate values based on lists
        self.store('average', self.get('total', 1) / self.get('count', 1))
        self.store('median', self.median())

    def min(self, name:str, value:int|float, data:dict = None):
        '''
        Store the smaller of two values, one being the existing stat vs the supplied. When a min
        is updated, also store additional data values if supplied.
        '''
        created = self._ensure(name, value)
        if value < self.stats[name] or created:
            self.stats[name] = value
            if data:
                for k,v in data.items():
                    self.stats[k] = v

    def max(self, name:str, value:int|float, data:dict = None):
        '''
        Store the larger of two values, one being the existing stat vs supplied. When a max is
        updated, also store additional data values if supplied.
        '''
        created = self._ensure(name, value)
        if self.stats[name] < value or created:
            self.stats[name] = value
            if data:
                for k,v in data.items():
                    self.stats[k] = v

    def median(self)-> float:
        ''' Calculate a median from the current values in 'list'. '''
        return statistics.median(self.get('list', []))

    def __str__(self) -> str:
        ''' Return a string representation of the stats. '''
        return str(self.stats)

    def dump(self) -> str:
        out = self.stats.copy()
        out['tests'] = []
        for test in self.subs:
            item = self.subs[test].stats
            item['name'] = test
            out['tests'].append(item)
        return json.dumps(out)

    def _sort_csv_headers(self, headers:list)->list:
        ''' note and name are row identifiers, put this first, but all others sort normally. '''
        priority_headers = ['note', 'name']
        for header in priority_headers:
            headers.remove(header)
        headers.sort()
        headers = priority_headers + headers
        return headers

    def csv(self, out_file):
        ''' Write out the stats to a csv file. '''
        headers = []
        for key in self.subs:
            headers = self._sort_csv_headers(list(self.subs[key].stats.keys()))
            break #just look at the first one, they are all the same
        if not 'valid' in headers:
            headers.append('valid')
        if not 'failed' in headers:
            headers.append('failed')
        with open(out_file, 'w', encoding="utf8") as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for sub in self.subs:
                data = self.subs[sub].stats.copy()
                data['name'] = sub
                writer.writerow(data)

# ################################################################################################ #
# testing

#s = Stats()
#print(s.max('test', 10, {'alt':3.14}))

def create_a_test_stats_object():
    # create a basic stats object for testing
    s = Stats()
    s.max('outer-test', 5)
    sub = s.get_sub("test1")
    sub.max('max', 10)
    sub.max('max', 5)
    sub.max('max', 15)
    sub = s.get_sub("test2")
    sub.max('max', 10)
    sub.max('max', 5)
    sub.max('max', 15)
    return s

#s = create_a_test_stats_object()
#print(s.csv('test.csv'))

# print(s.get_sub('test'))
# print('-'*10)
# print(s.dump())

# exit()
