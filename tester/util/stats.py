''' All functions related to the Stats object, an object to collect statistical information. '''
class Stats():
    ''' An object to track status in a dictionary with some convenience functions to manage them '''
    stats: dict

    def __init__(self):
        ''' Just getting things started with some defaults '''
        self.stats =  {}

    def _ensure(self, name, init_value) -> bool:
        '''
        Internal function to ensure that a value exists and has a default value before using it.
        Return True if the init_value was used, meaning the stat did not exist; True means created.
        '''
        if not name in self.stats:
            self.stats[name] = init_value
            return True
        return False

    def get(self, name, init_value):
        ''' Return a value stored, or use the initial value. '''
        self._ensure(name, init_value)
        return self.stats[name]

    def store(self, name, value):
        ''' Store a value in the stats overwriting any existing value. '''
        self.stats[name] = value

    def add(self, name, value):
        ''' Add a value to an existing stat. '''
        if not self._ensure(name, value):
            self.stats[name] = self.stats[name] + value


    def max(self, name, value, data:dict = None):
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

    def _max_id(self, name, value, name_id, id_value):
        ''' Remove? '''
        self._ensure(name, value)
        self._ensure(name_id, id_value)
        if self.stats[name] < value:
            self.stats[name] = value
            self.stats[name_id] = id

    def _data(self) -> dict:
        ''' Remove? '''
        return self.stats

    def dump(self) -> str:
        ''' Dump a string representation of the stats '''
        return str(self.stats)

#s = Stats()
#print(s.max('test', 10, {'alt':3.14}))
