
class Stats():
    ''' An object to track status '''
    stats: dict

    def __init__(self):
        self.stats =  {}

    def _ensure(self, name, init_value) -> bool:
        if not name in self.stats:
            self.stats[name] = init_value
            return True
        return False

    def get(self, name, init_value):
        self._ensure(name, init_value)
        return self[name]

    def add(self, name, value):
        self._ensure(name, value)
        self[name] = value

    def max(self, name, value, data:dict = None):
        created = self._ensure(name, value)
        if self.stats[name] < value or created:
            self.stats[name] = value
            if data:
                for k,v in data.items():
                    self.stats[k] = v

    def max_id(self, name, value, name_id, id):
        self._ensure(name, value)
        self._ensure(name_id, id)
        if self.stats[name] < value:
            self.stats[name] = value
            self.stats[name_id] = id

#s = Stats()
#print(s.max('test', 10, {'alt':3.14}))
