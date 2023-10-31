#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2023

"""
Main start entry point for iDDS service
"""


import logging
import signal
import time
import traceback

from idds.common.constants import Sections
from idds.common.config import config_has_section, config_has_option, config_list_options, config_get
from idds.common.utils import setup_logging, report_availability


setup_logging('idds.log')


AGENTS = {
    'baseagent': ['idds.agents.common.baseagent.BaseAgent', Sections.Common],
    'clerk': ['idds.agents.clerk.clerk.Clerk', Sections.Clerk],
    'marshaller': ['idds.agents.marshaller.marshaller.Marshaller', Sections.Marshaller],
    'transformer': ['idds.agents.transformer.transformer.Transformer', Sections.Transformer],
    'transporter': ['idds.agents.transporter.transporter.Transporter', Sections.Transporter],
    'submitter': ['idds.agents.carrier.submitter.Submitter', Sections.Carrier],
    'poller': ['idds.agents.carrier.poller.Poller', Sections.Carrier],
    'receiver': ['idds.agents.carrier.receiver.Receiver', Sections.Carrier],
    'trigger': ['idds.agents.carrier.trigger.Trigger', Sections.Carrier],
    'finisher': ['idds.agents.carrier.finisher.Finisher', Sections.Carrier],
    'conductor': ['idds.agents.conductor.conductor.Conductor', Sections.Conductor],
    'consumer': ['idds.agents.conductor.consumer.Consumer', Sections.Consumer],
    'archiver': ['idds.agents.archive.archiver.Archiver', Sections.Archiver],
    'coordinator': ['idds.agents.coordinator.coordinator.Coordinator', Sections.Coordinator]
}

RUNNING_AGENTS = []


def load_config_agents():
    if config_has_section(Sections.Main) and config_has_option(Sections.Main, 'agents'):
        agents = config_get(Sections.Main, 'agents')
        agents = agents.split(',')
        agents = [d.strip() for d in agents]
        return agents
    return []


def load_agent_attrs(section):
    """
    Load agent attributes
    """
    attrs = {}
    logging.info("Loading config for section: %s" % section)
    if config_has_section(section):
        options = config_list_options(section)
        for option, value in options:
            if not option.startswith('plugin.'):
                if isinstance(value, str) and value.lower() == 'true':
                    value = True
                if isinstance(value, str) and value.lower() == 'false':
                    value = False
                attrs[option] = value
    return attrs


def load_agent(agent):
    if agent not in AGENTS.keys():
        logging.critical("Configured agent %s is not supported." % agent)
        raise Exception("Configured agent %s is not supported." % agent)

    agent_cls, agent_section = AGENTS[agent]
    attrs = load_agent_attrs(agent_section)
    logging.info("Loading agent %s with class %s and attributes %s" % (agent, agent_cls, str(attrs)))

    k = agent_cls.rfind('.')
    agent_modules = agent_cls[:k]
    agent_class = agent_cls[k + 1:]
    module = __import__(agent_modules, fromlist=[None])
    cls = getattr(module, agent_class)
    impl = cls(**attrs)
    return impl


def run_agents():
    global RUNNING_AGENTS

    agents = load_config_agents()
    logging.info("Configured to run agents: %s" % str(agents))
    for agent in agents:
        agent_thr = load_agent(agent)
        RUNNING_AGENTS.append(agent_thr)

    for agent in RUNNING_AGENTS:
        agent.start()

    current = None
    while len(RUNNING_AGENTS):
        [thr.join(timeout=3.14) for thr in RUNNING_AGENTS if thr and thr.is_alive()]
        RUNNING_AGENTS = [thr for thr in RUNNING_AGENTS if thr and thr.is_alive()]
        if len(agents) != len(RUNNING_AGENTS):
            logging.critical("Number of active agents(%s) is not equal number of agents should run(%s)" % (len(RUNNING_AGENTS), len(agents)))
            logging.critical("Exit main run loop.")
            break

        if current is None or time.time() - current > 600:
            # select one agent to get the health items
            candidate = RUNNING_AGENTS[0]
            availability = candidate.get_availability()
            logging.debug("availability: %s" % availability)
            report_availability(availability)

            current = time.time()


def stop(signum=None, frame=None):
    global RUNNING_AGENTS

    logging.info("Stopping ......")
    logging.info("Stopping running agents: %s" % RUNNING_AGENTS)
    [thr.stop() for thr in RUNNING_AGENTS if thr and thr.is_alive()]
    stop_time = time.time()
    while len(RUNNING_AGENTS):
        [thr.join(timeout=1) for thr in RUNNING_AGENTS if thr and thr.is_alive()]
        RUNNING_AGENTS = [thr for thr in RUNNING_AGENTS if thr and thr.is_alive()]
        if time.time() > stop_time + 180:
            break

    logging.info("Still running agents: %s" % str(RUNNING_AGENTS))
    [thr.terminate() for thr in RUNNING_AGENTS if thr and thr.is_alive()]

    while len(RUNNING_AGENTS):
        logging.info("Still running agents: %s" % str(RUNNING_AGENTS))
        [thr.terminate() for thr in RUNNING_AGENTS if thr and thr.is_alive()]
        [thr.join(timeout=1) for thr in RUNNING_AGENTS if thr and thr.is_alive()]
        RUNNING_AGENTS = [thr for thr in RUNNING_AGENTS if thr and thr.is_alive()]


if __name__ == '__main__':

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGQUIT, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        run_agents()
        stop()
    except KeyboardInterrupt:
        stop()
    except Exception as error:
        logging.error("An exception is caught in main process: %s, %s" % (error, traceback.format_exc()))
        stop()
