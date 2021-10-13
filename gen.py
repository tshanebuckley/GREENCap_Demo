class MyClass():
    def __init__(self):
        self.mylist = []
    def add_request(self, x):
        self.mylist.append(x)
    def _generator(self):
        for item in self.mylist:
            yield self.mylist.pop()
    def next_request(self):
        if len(self.mylist) == 0:
            self.mylist.append(None)
        return next(self._generator())