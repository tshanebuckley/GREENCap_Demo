# import dependencies
import inspect

# class decorator to apply a decorator over prefixed methods
def for_all_methods_by_prefix(decorator, prefix=["import_", "export_", "delete_"]):
    def decorate(cls):
        for name, fn in name, fn in inspect.getmembers(MyProject, inspect.isroutine) if name.split('_')[0] in prefix]:
            setattr(cls, name, decorator(fn))
        return cls
    return decorate

# class decorator to apply a decorator over prefixed methods
def for_all_methods_by_name(decorator, name=[]):
    def decorate(cls):
        for name, fn in name, fn in inspect.getmembers(MyProject, inspect.isroutine) if name.split('_')[0] in name]:
            setattr(cls, name, decorator(fn))
        return cls
    return decorate