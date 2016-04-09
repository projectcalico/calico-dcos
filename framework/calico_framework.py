#!/usr/bin/env python

# Copyright 2015 Metaswitch Networks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""calico-dcos-installer-scheduler

Install calico in a DCOS cluster

Usage:
  calico_framework.py

Dockerized:
  docker run calico/calico-mesos-framework <args...>

Description:
  Add or remove containers to Calico networking, manage their IP addresses and profiles.
  All these commands must be run on the host that contains the container.
"""
import os
from random import randint
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from docopt import docopt
from calico_utils import _setup_logging
from tasks import (TaskUpdateError,
                   SleepTask)
from constants import LOGFILE, TASK_CPUS, TASK_MEM, \
    BAD_TASK_STATES

_log = _setup_logging(LOGFILE)
NEXT_AVAILABLE_TASK_ID = 0


class TestState(object):
    Unstarted, Running, Complete = range(0,3)


class Agent(object):
    def __init__(self, agent_id):
        self.agent_id = agent_id

        self.tasks = []
        """
        Tasks run on this agent.
        """

    def __repr__(self):
        return "Agent(%s)" % self.id

class CalicoScheduler(mesos.interface.Scheduler):
    def __init__(self):
        self.agents = []
        """
        A collection of TestCases this scheduler will run through should deploy.
        """


    def all_tasks(self):
        """
        Get all tasks across all tests.
        """
        for agent in self.agents:
            for task in agent.tasks:
                yield task


    def registered(self, driver, frameworkId, masterInfo):
        """
        Callback used when the framework is succesfully registered.
        """
        _log.info("REGISTERED: with framework ID %s", frameworkId.value)

    def resourceOffers(self, driver, offers):
        """
        Triggered when the framework is offered resources by mesos.
        """
        for offer in offers:
            # Get agent ID this offer belongs to
            # If that agent doesn't yet exist
                # Create a new agent object
            # Launch the next task this agent needs to launch
            # If we need to launch a task:
            task = SleepTask()
            global NEXT_AVAILABLE_TASK_ID
            task.task_id = str(NEXT_AVAILABLE_TASK_ID)
            NEXT_AVAILABLE_TASK_ID += 1
            # Store task state as staging (even though it wasn't reported as such) since we know we're launching the task
            task.slave_id = offer.slave_id.value

            operation = mesos_pb2.Offer.Operation()
            operation.launch.task_infos.extend([task.as_new_mesos_task()])
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            print "launching %s" % task
            driver.acceptOffers([offer.id], [operation])
            # else:
            # _log.info("\t\tNot tasks ready for this agent. Declining offer")
            # driver.declineOffer(offer.id)

    def statusUpdate(self, driver, update):
        """
        Triggered when the Framework receives a task Status Update from the
        Executor
        """
        # Find the task which corresponds to the status update
        try:
            calico_task = next(task for task in self.all_tasks() if
                               task.task_id == update.task_id.value)
        except StopIteration:
            _log.error(
                "FATAL: Received Task Update from Unidentified TaskID: %s",
                update.task_id.value)
            driver.abort()
            return

        try:
            calico_task.process_update(update)
        except TaskUpdateError as e:
            _log.error("This is bad.")
            pass


        _log.info("TASK_UPDATE - %s: %s",
                  mesos_pb2.TaskState.Name(calico_task.state),
                  calico_task)

        # Useful debugging dump from error statuses
        if calico_task.state in BAD_TASK_STATES:
            _log.error(
                "\t%s is in unexpected state %s with message '%s'",
                calico_task,
                mesos_pb2.TaskState.Name(update.state),
                update.message)
            _log.error("\tData:  %s", repr(str(update.data)))
            _log.error("\tSent by: %s",
                   mesos_pb2.TaskStatus.Source.Name(update.source))
            _log.error("\tReason: %s",
                   mesos_pb2.TaskStatus.Reason.Name(update.reason))
            _log.error("\tMessage: %s", update.message)
            _log.error("\tHealthy: %s", update.healthy)

            self.kill_test(calico_task.test, update.message)
            return


        # Check for good update
        if update.state == mesos_pb2.TASK_FINISHED:
            pass


class NotEnoughResources(Exception):
    pass


if __name__ == "__main__":
    # arguments = docopt(__doc__)
    master_ip = os.getenv('MASTER_IP', 'mesos.master')
    print "Connecting to Master: ", master_ip

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"

    framework.principal = "test-framework-python"

    scheduler = CalicoScheduler()

    _log.info("Launching")
    driver = mesos.native.MesosSchedulerDriver(scheduler,
                                               framework,
                                               master_ip)

    driver.start()
    driver.join()
