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

from idds.workflowv2.tree import JobNode, LabelNode, Tree

from .domaeventmap import DomaEventMap, DomaEventMapTask, DomaEventMapJob


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

    def group_label_level_dict(self, label_level_dict):
        grouped_label_level_dict = {}
        current_grouped_level, current_required_resource, current_number_jobs = 0, None, 0
        for level in label_level_dict:
            for node in label_level_dict[level]:
                whether_to_group = self.whether_to_group(node)
                node.whether_to_group = whether_to_group

                if not whether_to_group:
                    # if there is a previous group, close it
                    if current_number_jobs:
                        current_grouped_level += 1

                    grouped_label_level_dict[str(current_grouped_level)] = [node]
                    current_grouped_level += 1
                    current_required_resource, current_number_jobs = None, 0
                else:
                    num_jobs = len(node.jobs)
                    # self.logger.debug(node)
                    print(node)
                    max_events_per_job = self.get_max_events_per_job(node)
                    required_resource = "%s_%s_%s_%s_%s" % (node.compute_cloud, node.compute_site, node.queue, node.request_memory, max_events_per_job)

                    if str(current_grouped_level) not in grouped_label_level_dict:
                        grouped_label_level_dict[str(current_grouped_level)] = []

                    if not current_number_jobs:
                        # new job
                        current_required_resource = required_resource
                        current_number_jobs = num_jobs
                        grouped_label_level_dict[str(current_grouped_level)].append(node)
                    elif current_required_resource != required_resource or num_jobs + current_number_jobs > max_events_per_job:
                        # close the current group
                        current_grouped_level += 1
                        # create new group and wait for others to join this group
                        grouped_label_level_dict[str(current_grouped_level)] = [node]
                        current_required_resource = required_resource
                        current_number_jobs = num_jobs
                    # elif num_jobs >= max_events_per_job / 2:
                    #     # close the current group
                    #     current_grouped_level += 1
                    #     # create new group as a separate group
                    #     grouped_label_level_dict[str(current_grouped_level)] = [node]
                    #     # move to the next group
                    #     current_grouped_level += 1
                    #     current_required_resource, current_number_jobs = None, 0
                    else:
                        # group the current node to the previous group
                        grouped_label_level_dict[str(current_grouped_level)].append(node)
                        current_number_jobs += num_jobs
        return grouped_label_level_dict

    def whether_to_group(self, node):
        gwjob = node.one_gwjob
        if gwjob:
            return gwjob.attrs.get('grouping', True)
        return self._label_grouping.get(node.name, {}).get('grouping', True)

    def get_max_events_per_job(self, node):
        gwjob = node.one_gwjob
        if gwjob:
            return gwjob.attrs.get('grouping_max_jobs', 100)
        return self._label_grouping.get(node.name, {}).get('grouping_max_jobs', 100)

    def split_big_node(self, node, max_events_per_job=1000):
        job_nodes = node.jobs
        groups = {}
        for job_node in job_nodes:
            group_id = job_node.get_potential_group_id()
            if group_id not in groups:
                groups[group_id] = []
            groups[group_id].append(job_node)

        job_chunks = []
        for group_id in groups:
            group_jobs = groups[group_id]
            if len(group_jobs) > max_events_per_job:
                cluster_chunks = [group_jobs[i:i + max_events_per_job] for i in range(0, len(group_jobs), max_events_per_job)]
                job_chunks.extend(cluster_chunks)
            else:
                job_chunks.append(group_jobs)

        # merge job chunks
        merged_job_chunks = []
        current_job_chunk = None
        for job_chunk in job_chunks:
            if len(job_chunk) > max_events_per_job / 2:
                merged_job_chunks.append(job_chunk)
            else:
                if current_job_chunk is None:
                    current_job_chunk = job_chunk
                else:
                    if len(current_job_chunk) + len(job_chunk) <= max_events_per_job:
                        current_job_chunk.extend(job_chunk)
                    else:
                        merged_job_chunks.append(current_job_chunk)
                        current_job_chunk = job_chunk
        if current_job_chunk:
            merged_job_chunks.append(current_job_chunk)

        return merged_job_chunks

    def construct_grouped_jobs(self, grouped_label_level_dict):
        group_jobs, group_label, events, event_index = {}, None, {}, 0
        for level in grouped_label_level_dict:
            nodes = grouped_label_level_dict[level]
            # one level is one task
            group_label = "_".join([node.name for node in nodes])
            if len(nodes) > 1:
                # mulitple node to be merged into one job
                events, event_index = {}, 0
                group_id = "%s_0" % level
                event_file = "eventservice_" + group_label + "_" + group_id
                for node in nodes:
                    for job in node.jobs:
                        event_index_str = str(event_index)
                        event_index += 1
                        events[event_index_str] = job

                        job.group_label = group_label
                        job.event_file = event_file
                        job.event_index = event_index_str
                        job.group_id = group_id
                group_jobs[group_label] = [{'name': event_file, 'events': events}]
            else:
                # there is only one big node
                node = nodes[0]
                max_events_per_job = self.get_max_events_per_job(node)
                if len(node.jobs) <= max_events_per_job:
                    events, event_index = {}, 0
                    group_id = "%s_0" % level
                    event_file = "eventservice_" + group_label + "_" + group_id
                    for job in node.jobs:
                        event_index_str = str(event_index)
                        event_index += 1

                        events[event_index_str] = job

                        job.group_label = group_label
                        job.event_file = event_file
                        job.event_index = event_index_str
                        job.group_id = group_id
                    group_jobs[group_label] = [{'name': event_file, 'events': events}]
                else:
                    chunks = self.split_big_node(node, max_events_per_job)
                    group_jobs[group_label] = []
                    for i, chunk in enumerate(chunks):
                        events, event_index = {}, 0
                        group_id = "%s_%s" % (level, i)
                        event_file = "eventservice_" + group_label + "_" + group_id
                        for job in chunk:
                            event_index_str = str(event_index)
                            event_index += 1

                            events[event_index_str] = job
                            job.group_id = group_id

                            job.group_label = group_label
                            job.event_file = event_file
                            job.event_index = event_index_str
                        group_jobs[group_label].append({'name': event_file, 'events': events})

        return group_jobs

    def from_generic_workflow_combine(self, generic_workflow):
        job_tree_roots, job_nodes, label_jobs, label_parent_labels = self.get_job_tree(generic_workflow)
        self.job_tree_roots = job_tree_roots
        self.job_nodes = job_nodes
        self.label_jobs = label_jobs
        self.label_parent_labels = label_parent_labels
        print("job tree")
        print(job_tree_roots)
        print(job_nodes)
        print(label_jobs)
        print(label_parent_labels)

        label_tree_roots, label_nodes = self.get_label_tree(generic_workflow, label_parent_labels, label_jobs)
        self.label_tree_roots = label_tree_roots
        self.label_nodes = label_nodes
        print("label tree")
        print(label_tree_roots)
        print(label_nodes)

        label_level_dict = self.get_ordered_nodes_by_level(label_tree_roots)
        print("label_level_dict")
        print(label_level_dict)

        grouped_label_level_dict = self.group_label_level_dict(label_level_dict)
        print("grouped_label_level_dict")
        print(grouped_label_level_dict)

        # self.logger.debug(grouped_label_level_dict)
        grouped_jobs = self.construct_grouped_jobs(grouped_label_level_dict)
        return grouped_jobs

    def from_generic_workflow_width(self, generic_workflow):
        job_tree_roots, job_nodes, label_jobs, label_parent_labels = self.get_job_tree(generic_workflow)
        self.job_tree_roots = job_tree_roots
        self.job_nodes = job_nodes
        self.label_jobs = label_jobs
        self.label_parent_labels = label_parent_labels
        print("job tree")
        print(job_tree_roots)
        print(job_nodes)
        print(label_jobs)
        print(label_parent_labels)

        label_tree_roots, label_nodes = self.get_label_tree(generic_workflow, label_parent_labels, label_jobs)
        self.label_tree_roots = label_tree_roots
        self.label_nodes = label_nodes
        print("label tree")
        print(label_tree_roots)
        print(label_nodes)

        label_level_dict = self.get_ordered_nodes_by_level(label_tree_roots)
        print("label_level_dict")
        print(label_level_dict)

        grouped_label_level_dict = self.group_label_level_dict(label_level_dict)
        print("grouped_label_level_dict")
        print(grouped_label_level_dict)

        # self.logger.debug(grouped_label_level_dict)
        grouped_jobs = self.construct_grouped_jobs(grouped_label_level_dict)
        return grouped_jobs

    def from_generic_workflow(self, generic_workflow):
        if self.group_type == 'width':
            return self.from_generic_workflow_width(generic_workflow)
        return self.from_generic_workflow_combine(generic_workflow)

    def construct_map_between_jobs_and_events(self, job_nodes, grouped_jobs):
        job_event_map = {}
        for grouped_label in grouped_jobs:
            for eventservice in grouped_jobs[grouped_label]:
                name = eventservice['name']
                events = eventservice['events']
                for event_index in events:
                    job = events[event_index]
                    job_event_map[job.name] = {'group_label': grouped_label, 'event_job': name, 'event_index': event_index}
        for job_name in job_nodes:
            if job_name not in job_event_map:
                raise Exception("Job is not converted into EventService maps" % job_name)
        return job_event_map

    def construct_event_map(self, grouped_jobs, event_map_name=None):
        job_event_map = self.construct_map_between_jobs_and_events(self.job_nodes, grouped_jobs)

        event_map = DomaEventMap(event_map_name)
        for grouped_label in grouped_jobs:
            event_task = DomaEventMapTask(grouped_label)
            for eventservice in grouped_jobs[grouped_label]:
                name = eventservice['name']
                events = eventservice['events']
                event_job = DomaEventMapJob(grouped_label, name, events)
                event_job.construct_event_dependencies(job_event_map)

                event_task.add_job(event_job)
            event_map.add_task(event_task)
        event_map.save()
        return event_map

    def construct_idds_work(self, label, jobs, job_nodes):
        for job in jobs:
            name = job['name']
            events = job['events']
            construct_events = []
            for event_index in events:
                job_node = events[event_index]
                # gwjob = job_node.gwjob
                deps = job_node.deps
                construct_event = {'name': name, 'index': event_index, 'dependencies': {}}
                for dep_name in deps:
                    dep_job_node = job_nodes[dep_name]
                    dep = {'group_label': dep_job_node.group_label,
                           'event_file': dep_job_node.event_file,
                           'event_index': dep_job_node.event_index}
                    construct_event['dependencies'].append(dep)
                construct_events.append(construct_event)
            job['construct_events'] = construct_events
