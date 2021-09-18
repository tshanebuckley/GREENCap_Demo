# import dependencies
import inspect

# class decorator to apply a decorator over prefixed methods
def for_all_methods_by_prefix(decorator, prefix=["import_", "export_", "delete_"]):
    def decorate(cls):
        selected_classes = [{"name": name, "fn": fn} for name, fn in inspect.getmembers(cls, inspect.isroutine) if name.split('_')[0] in prefix]
        for selected_class in selected_classes:
            setattr(cls, selected_class["name"], decorator(selected_class["fn"]))
        return cls
    return decorate

# class decorator to apply a decorator over prefixed methods
def for_all_methods_by_name(decorator, names=[]):
    def decorate(cls):
        selected_classes = [{"name": name, "fn": fn} for name, fn in inspect.getmembers(MyProject, inspect.isroutine) if name.split('_')[0] in names]
        for selected_class in selected_classes:
            setattr(cls, selected_class["name"], decorator(selected_class["fn"]))
        return cls
    return decorate