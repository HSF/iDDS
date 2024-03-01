#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


import importlib
import inspect
import os
import sys
# import traceback

from contextlib import contextmanager
from typing import Any, Callable


@contextmanager
def add_cwd_path():
    """Context adding the current working directory to sys.path."""
    try:
        cwd = os.getcwd()
    except FileNotFoundError:
        cwd = None
    if not cwd:
        yield
    elif cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:  # pragma: no cover
                pass


def get_func_name(func: Callable, base_dir=None) -> str:
    """
    Return function name from a function.
    """
    filename = inspect.getfile(func)
    module = inspect.getmodule(func)
    module_name = module.__name__
    if base_dir is None:
        filename = os.path.basename(filename)
    else:
        filename = os.path.relpath(filename, base_dir)
    if 'site-packages' in filename:
        filename = filename.split('site-packages')
    if filename.startswith('/'):
        filename = filename[1:]
    return filename + ":" + module_name + ":" + func.__name__


def import_func(name: str) -> Callable[..., Any]:
    """Returns a function from a dotted path name. Example: `path.to.module:func`.

    When the attribute we look for is a staticmethod, module name in its
    dotted path is not the last-before-end word

    E.g.: filename:module_a.module_b:ClassA.my_static_method

    Thus we remove the bits from the end of the name until we can import it

    Args:
        name (str): The name (reference) to the path.

    Raises:
        ValueError: If no module is found or invalid attribute name.

    Returns:
        Any: An attribute (normally a Callable)
    """
    with add_cwd_path():
        filename, module_name_bits, attribute_bits = name.split(':')
        # module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
        if module_name_bits == '__main__':
            module_name_bits = filename.replace('.py', '').replace('.pyc', '')
            module_name_bits = module_name_bits.replace('/', '')
        module_name_bits = module_name_bits.split('.')
        attribute_bits = attribute_bits.split('.')
        module = None
        while len(module_name_bits):
            try:
                module_name = '.'.join(module_name_bits)
                module = importlib.import_module(module_name)
                break
            except ImportError:
                attribute_bits.insert(0, module_name_bits.pop())

        if module is None:
            # maybe it's a builtin
            try:
                return __builtins__[name]
            except KeyError:
                raise ValueError('Invalid attribute name: %s' % name)

        attribute_name = '.'.join(attribute_bits)
        if hasattr(module, attribute_name):
            return getattr(module, attribute_name)
        # staticmethods
        attribute_name = attribute_bits.pop()
        attribute_owner_name = '.'.join(attribute_bits)
        try:
            attribute_owner = getattr(module, attribute_owner_name)
        except:  # noqa
            raise ValueError('Invalid attribute name: %s' % attribute_name)

        if not hasattr(attribute_owner, attribute_name):
            raise ValueError('Invalid attribute name: %s' % name)
        return getattr(attribute_owner, attribute_name)


def import_attribute(name: str) -> Callable[..., Any]:
    """Returns an attribute from a dotted path name. Example: `path.to.func`.

    When the attribute we look for is a staticmethod, module name in its
    dotted path is not the last-before-end word

    E.g.: package_a.package_b.module_a.ClassA.my_static_method

    Thus we remove the bits from the end of the name until we can import it

    Args:
        name (str): The name (reference) to the path.

    Raises:
        ValueError: If no module is found or invalid attribute name.

    Returns:
        Any: An attribute (normally a Callable)
    """
    name_bits = name.split('.')
    module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
    module = None
    while len(module_name_bits):
        try:
            module_name = '.'.join(module_name_bits)
            module = importlib.import_module(module_name)
            break
        except ImportError:
            attribute_bits.insert(0, module_name_bits.pop())

    if module is None:
        # maybe it's a builtin
        try:
            return __builtins__[name]
        except KeyError:
            raise ValueError('Invalid attribute name: %s' % name)

    attribute_name = '.'.join(attribute_bits)
    if hasattr(module, attribute_name):
        return getattr(module, attribute_name)
    # staticmethods
    attribute_name = attribute_bits.pop()
    attribute_owner_name = '.'.join(attribute_bits)
    try:
        attribute_owner = getattr(module, attribute_owner_name)
    except:  # noqa
        raise ValueError('Invalid attribute name: %s' % attribute_name)

    if not hasattr(attribute_owner, attribute_name):
        raise ValueError('Invalid attribute name: %s' % name)
    return getattr(attribute_owner, attribute_name)
