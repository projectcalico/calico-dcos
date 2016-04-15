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

Install calico in a Mesos cluster.

Usage:
  calico_framework.py

Dockerized:
  docker run calico/calico-mesos-framework

Description:
  Mesos framework used to install Calico in a Mesos cluster..
"""
import json
import logging

import mesos.interface
import mesos.native
from kazoo.client import KazooClient, NoNodeError, NodeExistsError
from mesos.interface import mesos_pb2

from config import config
from tasks import (Task, TaskRunEtcdProxy, TaskInstallDockerClusterStore,
    TaskInstallNetmodules, TaskRunCalicoNode, TaskRunCalicoLibnetwork)

_log = logging.getLogger(__name__)


# TODO
# o  Need to check CPU/Mem for each calico task
# o  Check that a task can reboot the agent (quick test to make sure this will fly)
# o  Can we determine what slave the framework is running on?  That would help limit the issues
#    caused by restart - worst case scenario is we keep restarting the node that runs the framework...
#    kill one agent, framework moves to another, then we next that agent...and so on.  Shouldn't actually
#    matter, but it would slow things down.

# Need the following input parms:
# -  Number of agents that can be restarted at once
# -  Etcd SRV address
# -  Etcd proxy port (maybe)

TASK_ORDER = [
    TaskRunEtcdProxy,
    TaskInstallDockerClusterStore,
    TaskInstallNetmodules,
    TaskRunCalicoNode,
    TaskRunCalicoLibnetwork
]
TASKS_BY_CLASSNAME = {cls.__name__: cls for cls in TASK_ORDER}

# The ZooKeeper Calico agent directory
ZK_AGENT_DIR = "/calico-framework/agent"


class ZkDatastore(object):
    def __init__(self, url):
        self.url = url
        self.zk_prefix = "root"
        self._zk = KazooClient(hosts=url)
        self._zk.start()
        self._zk.ensure_path(ZK_AGENT_DIR)

    def load_tasks(self, agent_id):
        try:
            tasks_str, _stat = self._zk.get("%s/%s" % (ZK_AGENT_DIR, agent_id))
        except NoNodeError:
            tasks_dict = {}
        else:
            tasks_dict = json.loads(tasks_str)

        tasks = {
            cn: TASKS_BY_CLASSNAME[cn].from_dict(task_dict)
            for cn, task_dict in tasks_dict.iteritems()
        }
        return tasks

    def store_tasks(self, agent_id, tasks):
        """
        Store a group of tasks for an agent.
        :param tasks:
        :return:
        """
        tasks_dict = {
            cn: task.to_dict() for cn, task in tasks.iteritems()
        }
        tasks_json = json.dumps(tasks_dict)
        try:
            self._zk.create("%s/%s" % (ZK_AGENT_DIR, agent_id), tasks_json)
        except NodeExistsError:
            self._zk.set("%s/%s" % (ZK_AGENT_DIR, agent_id), tasks_json)


class Agent(object):
    def __init__(self, scheduler, agent_id):
        self.scheduler = scheduler
        """
        The Calico installer scheduler.
        """

        self.agent_id = agent_id
        """
        The agent ID.
        """

        self.agent_syncd = None
        """
        Whether this agent has sync'd with the Master.  This is a tri-state
        field (None, False, True).  None indicates a sync has not been
        initiated, False indicates it has started but not completed, True
        indicates complete.
        """

        self.tasks = {}
        """
        Tasks for each task type (keyed off the class name)
        """

        self.offers_suppressed = False
        """
        Whether offers are currently suppressed.
        """

    def __repr__(self):
        return "Agent(%s)" % self.agent_id

    def _task(self, cls):
        """
        Return the task associated with a Task class.
        :param cls:
        :return: The Task, or None if the task has not been scheduled.
        """
        return self.tasks.get(cls.__name__)

    def handle_offer(self, driver, offer):
        """
        Ask the agent if it wants to handle the supplied offer.
        :return:  None if the offer is not accepted, otherwise return the task
                  that needs scheduling.

        We ensure tasks are completed in a particular order:
        -  Firstly etcd-proxy needs to be running.
        -  Once etcd-proxy is installed we can install Docker multi-host
           networking (this will restart Docker if necessary)
        -  Once Docker is updated, we can install net-modules and Calico plugin
           on the agent (this will restart the Agent if necessary)
        -  Then Calico node and Calico libetwork driver can be started.

        All failed tasks are rescheduled until:
        -  Non-persistent tasks finish successfully
        -  Persistent tasks are in a running state.

        A note on component restarts
        ============================

        Since we are possibly restarting docker and/or the agent, we
        need to consider what happens to these tasks when the services
        restart.

        1) Restarting Docker may cause the framework to exit (since it is
        running as a docker container).  If that is the case the framework
        will be restarted and existing tasks will be re-queried and any failed
        persistent tasks will be restarted.  All tasks are idempotent.  Some
        tasks restart components - they are written in such a way that won't
        keep restarting components if it isn't necessary.

        2) Restarting the agent could also cause the framework to exit in which
        case the same logic applies as for a Docker restart.  Also, the task
        that the agent was running may appear as failed.  If we get a failed
        task, we re-run it.

        In both cases, once a restart has successfully applied the new config
        the tasks ensure that we do not restart again - so we will not end up
        in a restart loop.
        """
        _log.debug("Handling offer")

        if self.agent_syncd is None:
            # A resync has not been initiated.  Trigger a resync.
            _log.debug("Agent is not yet sync'd - trigger a resync")
            self.trigger_resync(driver)

        # If there is nothing to resync, the agent_syncd flag may be set,
        # so check it again to decide whether we have to wait.
        if not self.agent_syncd:
            _log.debug("Agent has not yet sync'd")
            return None

        # Loop through the tasks in required order.  If a task needs scheduling
        # then schedule it if we can.  If we can't schedule the required task
        # then exit - we will attempt to schedule next offer.
        for task_class in TASK_ORDER:
            _log.debug("Handling task: '%s'", task_class.__name__)
            if not task_class.allowed():
                _log.debug("Task is not allowed - skip")

            if self.task_needs_scheduling(task_class):
                _log.debug("Task needs scheduling")
                if self.task_can_be_offered(task_class, offer):
                    _log.debug("Task can be offered")
                    return self.new_task(task_class)
                else:
                    _log.debug("Task cannot be offered - wait")
                    return None

        _log.debug("All tasks are running, suppress offers")
        self.offers_suppressed = True
        driver.suppressOffers()
        return None

    def trigger_resync(self, driver):
        """
        Trigger a resync of the tasks persisted in ZooKeeper.
        :param driver:
        :return:
        """
        if not self.scheduler.zk:
            _log.info("No ZK for persistent state.")
            self.agent_syncd = True
            return

        # Query the state of each task that was previously running.  Terminated
        # tasks do not need to be queried because their state won't have
        # changed, and the task may have been tidied up.
        self.tasks = self.scheduler.zk.load_tasks(self.agent_id)
        task_statuses = [task.get_task_status(self)
                         for task in self.tasks.itervalues() if task.running()]
        if not task_statuses:
            _log.info("No running tasks, assume sync'd")
            self.agent_syncd = True
            return

        # Start the resync and indicate that it is not yet complete.
        driver.reconcileTasks(task_statuses)
        self.agent_syncd = False

    def new_task(self, task_class, *args, **kwargs):
        """
        Create a new Task of the supplied type, and update our cache to store
        the task.
        :param task_class:
        :return: The new task.
        """
        task = task_class(*args, **kwargs)
        self.tasks[task_class.__name__] = task

        # Persist these tasks to the datastore.
        if self.scheduler.zk:
            self.scheduler.zk.store_tasks(self.agent_id, self.tasks)

        return task

    def task_can_be_offered(self, task_class, offer):
        """
        Whether a task can be included in the offer request.  A task can be
        included when the following conditions are met:
         -  the task resource requirements are fulfilled by the offer
         -  the task is either not a restart task, or we have not exceeded
            our quota of concurrent restart tasks.
        :param task_class:
        :param offer:
        :return:
        """
        return (task_class.can_accept_offer(offer) and
                (not task_class.restarts or
                 self.scheduler.can_restart_agent(self)))

    def task_needs_scheduling(self, task_class):
        """
        Whether a task needs scheduling.  A task needs scheduling if:
         -  it has not yet been run
         -  the hash of the previous run is different to the hash of the
            current task
         -  if the task failed
         -  if the task is persistent, but the current state indicates that it
            is not running.

        :param task_class:
        :return: True if the task needs scheduling.  False otherwise.
        """
        task = self._task(task_class)
        if not task:
            _log.debug("Task '%s' has not yet been run", task_class)
            return True

        if task.hash != task_class.hash:
            _log.debug("Task '%s' has been changed.  Prev=%s, Curr=%s",
                       task_class, task.hash, task_class.hash)
            return True

        if task.failed():
            _log.debug("Task '%s' failed", task_class)

        if task.persistent and task.running():
            _log.debug("Task '%s' is not running", task_class)
            return True

        _log.debug("Task '%s' does not need scheduling", task_class)
        return False

    def task_running(self, task_class):
        """
        Return if a task is running or not.
        :param task_class:
        :return:
        """
        task = self._task(task_class)
        if not task:
            return False
        return task.running()

    def task_finished(self, task_class):
        """
        Return if a task is finished or not.
        :param task_class:
        :return:
        """
        task = self._task(task_class)
        if not task:
            return False
        return task.finished()

    def handle_update(self, driver, update):
        """
        Handle a task update.  If we were resync-ing then check if we have
        completed the sync.
        :param driver:
        :param update:
        """
        # Extract the task classname from the task ID and update the task.
        classname = Task.classname_from_task_id(update.task_id.value)

        # Lookup the existing task, if there is one.  Only update if the task
        # ID matches the one in our cache.
        task = self.tasks.get(classname)
        if not task or task.task_id != update.task_id.value:
            _log.debug("Task is not recognised - ignoring")
            return
        _log.debug("TASK_UPDATE - %s: %s",
                   mesos_pb2.TaskState.Name(update.state),
                   task)

        # Update the task.
        task.update(update)

        # We expect restart tasks to fail, but not others.
        if task.failed() and not task.restarts:
            _log.error("\t%s is in unexpected state %s with message '%s'",
                   task, mesos_pb2.TaskState.Name(update.state), update.message)
            _log.error("\tData:  %s", repr(str(update.data)))
            _log.error("\tSent by: %s", mesos_pb2.TaskStatus.Source.Name(update.source))
            _log.error("\tReason: %s", mesos_pb2.TaskStatus.Reason.Name(update.reason))
            _log.error("\tMessage: %s", update.message)
            _log.error("\tHealthy: %s", update.healthy)

        # Persist the updated tasks to the datastore.
        if self.scheduler.zk:
            self.scheduler.zk.store_tasks(self.agent_id, self.tasks)

        # If we were resyncing then check if all of our tasks are now syncd.
        if not self.agent_syncd and all(task.clean for task in self.tasks.values()):
            self.agent_syncd = True

        # If offers were suppressed, then revive them now since something has
        # changed.
        if self.offers_suppressed:
            _log.debug("Revive offers for agent")
            driver.reviveOffers()
            self.offers_suppressed = False

    def is_restarting(self):
        """
        Return whether a component restart is in progress.  A restart is in
        progress when a restart task is not finished.
        :return: True if any restart task is in progress.
        """
        return any(t.restarts and not t.finished() for t in self.tasks.values())


class CalicoInstallerScheduler(mesos.interface.Scheduler):
    max_num_concurrent_restart = 2
    def __init__(self):
        self.agents = {}
        self.zk = ZkDatastore(config.zk_persist_url)

    def can_restart_agent(self, agent):
        """
        Determine if we are allowed to trigger an agent restart.

        We only allow a maximum number of agents to be restarted at the same
        time.  We can ignore the requesting agent in the count.

        :param agent:
        :return: True if the agent can be restarted, False otherwise.
        """
        num_restarting = sum(1 for a in self.agents.values() if a.is_restarting()
                               if a != agent)
        return num_restarting < self.max_num_concurrent_restart

    def get_agent(self, agent_id):
        """
        Return the Agent based on the agent ID.  If the agent is not in our
        cache then create an entry for it.
        :param agent_id:
        :return:
        """
        agent = self.agents.get(agent_id)
        if not agent:
            agent = Agent(self, agent_id)
            self.agents[agent_id] = agent
        return agent

    def registered(self, driver, frameworkId, masterInfo):
        """
        Callback used when the framework is successfully registered.
        """
        _log.info("REGISTERED: with framework ID %s", frameworkId.value)
        # self.zk.set_framework_id(frameworkId.value)

    def reregistered(self, driver, frameworkId, masterInfo):
        """
        Callback used when the framework is successfully re-registered.

        We mark all agents as un-sync'd to force a resync of the local cache.
        """
        _log.info("REREGISTERED: with framework ID %s", frameworkId.value)
        for agent in self.agents.itervalues():
            agent.agent_syncd = False

    def resourceOffers(self, driver, offers):
        """
        Triggered when the framework is offered resources by mesos.
        """
        # Extract the task ID.  The format of the ID includes the ID of the
        # agent it is running on.
        for offer in offers:
            agent = self.get_agent(offer.slave_id.value)
            task = agent.handle_offer(driver, offer)
            if not task:
                driver.declineOffer(offer.id)
                continue

            _log.info("Launching Task %s", task)
            mesos_task = task.as_new_mesos_task(offer.slave_id.value)
            operation = mesos_pb2.Offer.Operation()
            operation.launch.task_infos.extend([mesos_task])
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        """
        Triggered when the Framework receives a task Status Update from the
        Executor
        """
        # Pass the update to the appropriate Agent.
        agent = self.get_agent(update.slave_id.value)
        agent.handle_update(driver, update)

    def slaveLost(self, driver, slave_id):
        """
        When a slave is lost, mark it as un-sync'd so that we end up requerying
        the current state of existing tasks.
        :param driver:
        :param slave_id:
        """
        agent = self.get_agent(slave_id)
        agent.agent_syncd = None

    def offerRescinded(self, driver, offer_id):
        # Not obvious if this is actually required.  We probably need to track
        # offer to speed things up, but I don't believe it's strictly necessary.
        _log.info("Offer rescinded for offer ID %s", offer_id)


def launch_framework():
    """
    Launch the Calico framework.
    :return: The Mesos driver.  The caller should call driver.join() to ensure
    thread is blocked until driver exits.
    """
    master_addr = "{}:{}".format(config.master_ip, config.master_port)
    _log.info("Connecting to Master: %s", master_addr)
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "root"  # Have Mesos fill in the current user.
    framework.name = "Calico framework"
    framework.principal = "calico-framework"
    framework.id.value = "calico-framework"
    framework.failover_timeout = 604800

    _log.info("Launching Calico Mesos scheduler")
    scheduler = CalicoInstallerScheduler()
    driver = mesos.native.MesosSchedulerDriver(scheduler,
                                               framework,
                                               master_addr)
    driver.start()
    return driver


if __name__ == "__main__":
    _log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s]\t%(name)s %(lineno)d: %(message)s')

    # Create Console Logger
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    _log.addHandler(handler)
    fdriver = launch_framework()
    fdriver.join()
    fdriver.join()
