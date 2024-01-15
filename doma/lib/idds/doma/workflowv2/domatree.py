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
Construct tree from a generic workflow
"""

import json

from idds.workflowv2.tree import JobNode, LabelNode, Tree


class DomaTree(Tree):
    def __init__(self, name, group_type='combine', **kwargs):
        super(DomaTree, self).__init__(name, **kwargs)
        self._label_grouping = {'default': {'grouping': True, 'grouping_max_jobs': 100}}
        self.job_tree_roots = None
        self.job_nodes = None
        self.label_jobs = None
        self.label_parent_labels = None

        self.label_tree_roots = None
        self.label_nodes = None
        self.group_type = group_type

    def set_grouping_attribute(self, label, grouping, max_jobs):
        self._label_grouping[label] = {'grouping': grouping, 'max_jobs': max_jobs}

    def get_job_tree(self, generic_workflow):
        job_tree_roots = []
        job_nodes = {}

        label_jobs = {}
        label_parent_labels = {}
        for job_label in generic_workflow.labels:
            if job_label not in label_jobs:
                label_jobs[job_label] = []
                label_parent_labels[job_label] = []
            jobs_by_label = generic_workflow.get_jobs_by_label(job_label)
            for gwjob in jobs_by_label:
                deps = []
                for parent_job_name in generic_workflow.predecessors(gwjob.name):
                    # deps.append(parent_job_name)
                    parent_job = generic_workflow.get_job(parent_job_name)
                    deps.append(parent_job)
                    if parent_job.label not in label_parent_labels[job_label]:
                        label_parent_labels[job_label].append(parent_job.label)

                node = JobNode(gwjob.name, label=job_label, gwjob=gwjob, deps=deps)
                label_jobs[job_label].append(node)
                job_nodes[gwjob.name] = node

                if not node.deps:
                    job_tree_roots.append(node)
                else:
                    for dep in node.deps:
                        dep_node = job_nodes[dep.name]
                        dep_node.add_child(node)
        return job_tree_roots, job_nodes, label_jobs, label_parent_labels

    def get_label_tree(self, generic_workflow, label_parent_labels, label_jobs):
        label_tree_roots, label_nodes = [], {}
        for job_label in generic_workflow.labels:
            # jobs_by_label = generic_workflow.get_jobs_by_label(job_label)
            jobs_by_label = label_jobs[job_label]
            one_gwjob = jobs_by_label[0].gwjob
            label_node = LabelNode(job_label, jobs=jobs_by_label, compute_cloud=one_gwjob.compute_cloud,
                                   compute_site=one_gwjob.compute_site, queue=one_gwjob.queue,
                                   request_memory=one_gwjob.request_memory,
                                   one_gwjob=one_gwjob)
            label_nodes[job_label] = label_node
            if not label_parent_labels[job_label]:
                label_tree_roots.append(label_node)
            else:
                for parent_label in label_parent_labels[job_label]:
                    parent_node = label_nodes[parent_label]
                    parent_node.add_child(label_node)

        # set level here
        for root in label_tree_roots:
            root.level = 0
        return label_tree_roots, label_nodes

    def get_ordered_nodes_by_level(self, roots):
        level_dict = {}
        has_nodes = True
        left_nodes = roots
        while has_nodes:
            nodes = left_nodes
            left_nodes = []
            has_nodes = False
            for node in nodes:
                if node.level not in level_dict:
                    level_dict[node.level] = []
                level_dict[node.level].append(node)
                if node.children:
                    left_nodes.extend(node.children)
            if left_nodes:
                has_nodes = True

        return level_dict

    def order_job_nodes(self, job_nodes):
        job_nodes_order_id = {}
        for job_node in job_nodes:
            potential_order_id = job_node.get_potential_order_id()
            if potential_order_id not in job_nodes_order_id:
                job_nodes_order_id[potential_order_id] = []
            job_nodes_order_id[potential_order_id].append(job_node)
        potential_order_ids = sorted(list(job_nodes_order_id.keys()))
        ordered_job_nodes = []
        for potential_order_id in potential_order_ids:
            p_job_nodes = job_nodes_order_id[potential_order_id]
            for p_job_node in p_job_nodes:
                ordered_job_nodes.append(p_job_node)
        order_id = 0
        for job_node in ordered_job_nodes:
            job_node.order_id = order_id
            gwjob = job_node.gwjob
            # gwjob.order_id = order_id
            gwjob.attrs["order_id"] = order_id
            order_id += 1

    def order_job_tree(self, label_level_dict):
        for level in label_level_dict:
            for node in label_level_dict[level]:
                label_node = node
                # label_name = label_node.name
                job_nodes = label_node.jobs
                self.order_job_nodes(job_nodes)

    def generate_order_id_map(self, label_level_dict):
        order_id_map = {}
        for level in label_level_dict:
            for node in label_level_dict[level]:
                label_node = node
                label_name = label_node.name
                job_nodes = label_node.jobs
                order_id_map[label_name] = {}
                for job_node in job_nodes:
                    gwjob = job_node.gwjob
                    order_id = gwjob.attrs.get("order_id", 0)
                    order_id_map[label_name][str(order_id)] = gwjob.name
        return order_id_map

    def save_order_id_map(self, order_id_map, order_id_map_file):
        with open(order_id_map_file, 'w') as f:
            json.dump(order_id_map, f)

    def order_jobs_from_generic_workflow(self, generic_workflow, order_id_map_file):
        job_tree_roots, job_nodes, label_jobs, label_parent_labels = self.get_job_tree(generic_workflow)
        self.job_tree_roots = job_tree_roots
        self.job_nodes = job_nodes
        self.label_jobs = label_jobs
        self.label_parent_labels = label_parent_labels

        label_tree_roots, label_nodes = self.get_label_tree(generic_workflow, label_parent_labels, label_jobs)
        self.label_tree_roots = label_tree_roots
        self.label_nodes = label_nodes

        label_level_dict = self.get_ordered_nodes_by_level(label_tree_roots)

        self.order_job_tree(label_level_dict)
        # self.save_order_id_map(label_level_dict, order_id_map_file)
        order_id_map = self.generate_order_id_map(label_level_dict)
        return order_id_map
