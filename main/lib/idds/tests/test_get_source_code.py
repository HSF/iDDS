import dill
import inspect


def foo(arg1, arg2):
    # do something with args
    a = arg1 + arg2
    return a


source_foo = inspect.getsource(foo)  # foo is normal function
print(source_foo)


# source_max = inspect.getsource(max)  # max is a built-in function
# print(source_max)

print(inspect.signature(foo))
print(dill.dumps(foo))
