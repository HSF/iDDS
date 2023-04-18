#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023


def merge_dict(dict1, dict2):
    print("merge: %s, %s" % (dict1, dict2))
    # keys = list(dict1.keys()) + list(dict2.keys())
    keys = list(dict1.keys())
    for key in list(dict2.keys()):
        if key not in keys:
            keys.append(key)
    for key in keys:
        if key in dict2:
            if key not in dict1 or dict1[key] is None:
                dict1[key] = dict2[key]
            else:
                if dict2[key] is None:
                    continue
                elif isinstance(dict1[key], type(dict2[key])):
                    raise Exception("type of %s is different from %s, cannot merge" % (type(dict1[key]), type(dict2[key])))
                elif dict1[key] == dict2[key]:
                    continue
                elif type(dict1[key]) in (list, tuple, str):
                    dict1[key] = dict1[key] + dict2[key]
                elif type(dict1[key]) in (int, float, complex):
                    dict1[key] = dict1[key] + dict2[key]
                elif type(dict1[key]) in (bool, bool):
                    dict1[key] = True
                elif type(dict1[key]) in (dict, dict):
                    dict1[key] = merge_dict(dict1[key], dict2[key])
    return dict1


if __name__ == '__main__':
    a = {'a': 1, 'b': 2, 'c': [1, 2], 'd': {'a': 1, 'c': [4, 5]}, 'f': 1332}
    b = {'a': 3, 'b': 4, 'c': [3, 4], 'd': {'b': 1, 'c': [6, 5]}, 'e': 'abd'}
    print(id(a))
    print(id(b))
    c = merge_dict(a, b)
    print(id(c))
    print(c)
