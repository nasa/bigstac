''' All functions related to the Stats object, an object to collect statistical information. '''

import statistics

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
        ''' Dump a string representation of the stats and subs '''
        out = str(self.stats)
        for i in self.subs:
            out += '\n' + i + "=" + str(self.subs[i])
        return out

#s = Stats()
#print(s.max('test', 10, {'alt':3.14}))

# s = Stats()
# s.max('outer-test', 5)
# sub = s.get_sub("test")
# sub.max('inner-test1', 10)
# sub.max('inner-test1', 5)
# sub.max('inner-test1', 15)
# sub.max('inner-test2', 10)
# sub.max('inner-test2', 5)
# sub.max('inner-test2', 15)

# print(s)
# print(s.get_sub('test'))
# print('-'*10)
# print(s.dump())

# exit()
