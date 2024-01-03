#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023


"""
Construct tree from a workflow
"""

from idds.common.utils import setup_logging

from .base import Base


setup_logging(__name__)


class Node(Base):
    def __init__(self, name, children=None, parents=None, index=None, level=None, groups=None, disable_grouping=False, **kwargs):
        self.__dict__.update(kwargs)

        if children is None:
            children = []
        if parents is None:
            parents = []
        if groups is None:
            groups = []
        self.name = name
        self.index = index
        self.level = level
        self.children = children
        self.parents = parents
        self.groups = []
        self.order_groups = []
        self.group_id = None
        self.order_id = None
        self.disable_grouping = disable_grouping

        self.setup_logger()

    @property
    def index(self):
        return str(self.__index)

    @index.setter
    def index(self, value):
        if value:
            self.__index = str(value)
        else:
            self.__index = value

    @property
    def level(self):
        return str(self.__level)

    @level.setter
    def level(self, value):
        if value is not None:
            if self.__level is None or (self.__level is not None and value > self.__level):
                self.__level = value
                for child in self.children:
                    child.level = value + 1
        else:
            self.__level = value

    def add_child(self, obj):
        self.children.append(obj)
        obj.parents.append(self)

    def add_group(self, group):
        if group and group not in self.groups:
            self.groups.append(group)
            for child in self.children:
                if not child.group_id:
                    child.add_group(group)
            for parent in self.parents:
                if not parent.group_id:
                    parent.add_group(group)

    def get_potential_group_id(self):
        if not self.groups:
            return "None"
        return "_".join([str(i) for i in sorted(self.groups)])

    @property
    def group_id(self):
        return self.__group_id

    @group_id.setter
    def group_id(self, group_id):
        self.__group_id = group_id
        if group_id:
            for child in self.children:
                if not child.group_id:
                    child.add_group(group_id)
            for parent in self.parents:
                if not parent.group_id:
                    parent.add_group(group_id)

    def add_order_group(self, group):
        if group is not None and group not in self.order_groups:
            self.order_groups.append(group)
            for child in self.children:
                if child.order_id is None:
                    child.add_order_group(group)
            for parent in self.parents:
                if parent.order_id is None:
                    parent.add_order_group(group)

    def get_potential_order_id(self):
        if not self.order_groups:
            return 0
        return "_".join([str(i) for i in sorted(self.order_groups)])

    @property
    def order_id(self):
        return self.__order_id

    @order_id.setter
    def order_id(self, value):
        self.__order_id = value
        if value is not None:
            self.__order_id = int(value)
            for child in self.children:
                if child.order_id is None:
                    child.add_order_group(value)
            for parent in self.parents:
                if parent.order_id is None:
                    parent.add_order_group(value)

    def get_node_name(self):
        return get_node_name(self.index, self.name)

    def __repr__(self):
        # if self.__level:
        #     return "_" * self.__level + "Node(name: %s, level: %s)" % (self.name, self.__level)
        # return "Node(name: %s, level: %s, group_id: %s, groups: %s, parents: %s, children: %s)" % (self.name, self.__level, self.group_id, self.groups, self.parents, self.children)
        return "Node(name: %s, level: %s)" % (self.name, self.__level)


class WorkNode(Node):
    def __init__(self, name, work=None, children=None, parents=None, index=None, level=None, groups=None,
                 disable_grouping=False, **kwargs):
        super(WorkNode, self).__init__(name=name, work=work, children=children, parents=parents,
                                       index=index, level=level, groups=groups,
                                       disable_grouping=disable_grouping, **kwargs)

    def __repr__(self):
        return "WorkNode(name: %s, level: %s)" % (self.name, self.level)


class LabelNode(Node):
    def __init__(self, name, jobs=None, children=None, parents=None, index=None, level=None, groups=None,
                 disable_grouping=False, **kwargs):
        super(LabelNode, self).__init__(name=name, jobs=jobs, children=children, parents=parents,
                                        index=index, level=level, groups=groups,
                                        disable_grouping=disable_grouping, **kwargs)

    def __repr__(self):
        return "LabelNode(name: %s, level: %s)" % (self.name, self.level)


class JobNode(Node):
    def __init__(self, name, work_node=None, children=None, parents=None, index=None, level=None,
                 groups=None, disable_grouping=False, **kwargs):
        super(JobNode, self).__init__(name=name, work=work_node, children=children, parents=parents,
                                      index=index, level=level, groups=groups,
                                      disable_grouping=disable_grouping, **kwargs)

    def __repr__(self):
        return "JobNode(name: %s, level: %s)" % (self.name, self.level)

    def get_node_name(self):
        index = self.index
        if self.work_node:
            index = self.work_node.index
        return get_node_name(index, self.name)


def get_node_name(index=None, name=None):
    if index:
        return "%s:%s" % (index, name)
    return name


class Tree(Base):
    def __init__(self, name, **kwargs):
        self.__dict__.update(kwargs)
        self.name = name
        self.roots = []

        self.setup_logger()
