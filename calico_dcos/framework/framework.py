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
import os

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2
from kazoo.client import KazooClient, NoNodeError, NodeExistsError

from tasks import (Task, TaskRunEtcdProxy, TaskInstallDockerClusterStore,
                   TaskInstallNetmodules, TaskRestartComponents,
                   TaskRunCalicoNode, TaskRunCalicoLibnetwork)
from calico_dcos.common.utils import setup_logging
from calico_dcos.common.constants import LOGFILE_FRAMEWORK

_log = setup_logging(LOGFILE_FRAMEWORK)

# TODO
# o  Need to check CPU/Mem for each calico task
# o  Check that a task can reboot the agent (quick test to make sure this will fly)
# o  What happens if a framework is killed
# o  Can we determine what slave the framework is running on?  That would help limit the issues
#    caused by restart - worst case scenario is we keep restarting the node that runs the framework...
#    kill one agent, framework moves to another, then we next that agent...and so on.  Shouldn't actually
#    matter, but it would slow things down.
# o  Framework could ultimately have a web UI - so launch with a known DNS entry so you can point
#    a browser to determine current calico status.  Tasks could perform periodic checks on calico
# o  Suppress offers when calico services are running
# o  Revive offers when calico services are not running

# Need the following input parms:
# -  Number of agents that can be restarted at once
# -  Etcd SRV address
# -  Etcd proxy port (maybe)

TASK_CLASSES = [TaskRunEtcdProxy, TaskInstallNetmodules,
                TaskInstallDockerClusterStore, TaskRestartComponents,
                TaskRunCalicoNode, TaskRunCalicoLibnetwork]
TASK_CLASSES_BY_ACTION = {cls.action: cls for cls in TASK_CLASSES}

# The ZooKeeper Calico agent directory
ZK_AGENT_DIR = "/calico-framework/agent"

class ZkDatastore(object):
    def __init__(self, url):
        self.url = url
        self.zk_prefix = "calico"
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
            action: TASK_CLASSES_BY_ACTION[action].from_dict(task_dict)
            for action, task_dict in tasks_dict.iteritems()
        }
        return tasks

    def store_tasks(self, agent_id, tasks):
        """
        Store a group of tasks for an agent.
        :param tasks:
        :return:
        """
        tasks_dict = {
            action: task.to_dict() for action, task in tasks.iteritems()
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
        Tasks for each task type (we only ever have one of each running on an
        agent.
        """

        self.restarting = False
        """
        Whether this agent has initiated a restart sequence.  Once set, this
        is reset when a restart is no longer required.
        """

    def __repr__(self):
        return "Agent(%s)" % self.agent_id

    def _task(self, cls):
        """
        Return the task associated with a Task class.
        :param cls:
        :return: The Task, or None if the task has not been scheduled.
        """
        return self.tasks.get(cls.action)

    def handle_offer(self, driver, offer):
        """
        Ask the agent if it wants to handle the supplied offer.
        :return:  None if the offer is not accepted, otherwise return the task
                  that needs scheduling.

        Installation tasks need to be performed in a particular order:
        -  Firstly etcd-proxy needs to be running.  In parallel with this we
           can install netmodules (with Calico plugin).
        -  Once etcd-proxy is installed we can update the Docker configuration to
           use etcd-proxy as its cluster store (we don't need to wait for the
           netmodules install to complete).
        -  If the netmodules or docker tasks indicated that a restart is
           required then restart the appropriate componenets.  See note below.
        -  Once Docker and Agent are restarted, we can spin up the Calico
           node and the Calico libnetwork driver.

        A note on component restarts
        ============================

        Whether or not we restart anything is handled by the install tasks for
        docker multihost networking and for netmodules.  We could make the
        restart check to see if anything needs restarting - and simply no-op if
        nothing needs restarting.  However, since we want to limit how many
        agents are restarting at any one time, this slows down how quickly we
        can perform the subsequent steps.  Instead, we have the install tasks
        indicate whether a restart is required.  If a restart is required we
        will do the restart, otherwise we won't - thus agents that don't need a
        restart will not get blocked behind an agent installation that does.

        Since we are possibly restarting docker and/or the agent, we
        need to consider what happens to these tasks when the services
        restart.

        1) Restarting Docker may cause the framework to exit (since it is
        running as a docker container).  If that is the case the framework
        will be restarted and will kick off the install sequence again on each
        agent.  Once installed they will not be re-installed (and therefore
        the systems will not be restarted).

        2) Restarting the agent could also cause the framework to exit in which
        case the same logic applies as for a Docker restart.  Also, the task
        that the agent was running may appear as failed.  If we get a failed
        notification from a restart task, we re-run the installation tasks.

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

        if self.task_can_be_offered(TaskRunEtcdProxy, offer):
            # We have no etcd task running - start one now.
            _log.info("Install and run etcd proxy")
            return self.new_task(TaskRunEtcdProxy)

        if self.task_can_be_offered(TaskInstallNetmodules, offer):
            # We have not yet installed netmodules - do that now (we don't need
            # to wait for etcd to come online).
            _log.info("Install or check netmodules")
            return self.new_task(TaskInstallNetmodules)

        if not self.task_running(TaskRunEtcdProxy):
            # We need to wait for the proxy to come online before continuing.
            _log.info("Waiting for etcd proxy to be healthy")
            return None

        if self.task_can_be_offered(TaskInstallDockerClusterStore, offer):
            # Etcd proxy is running, so lets make sure Docker is configured to
            # use etcd as its cluster store.
            _log.info("Install or check docker multihost networking config")
            return self.new_task(TaskInstallDockerClusterStore)

        if not self.task_finished(TaskInstallNetmodules):
            # If we are waiting for successful completion of the netmodules
            # install then do not continue.
            _log.info("Waiting for netmodules installation")
            return None

        if not self.task_finished(TaskInstallDockerClusterStore):
            # If we are waiting for successful completion of the docker multi
            # host networking install then do not continue.
            _log.info("Waiting for docker networking installation")
            return None

        # Determine if a restart is required.
        # -  If a restart is required, kick off the restart task.
        # -  Otherwise, make sure our restarting flag is reset, and continue
        #    with the rest of the installation.
        restart_options = self._task(TaskInstallNetmodules).restart_options() | \
                          self._task(TaskInstallDockerClusterStore).restart_options()
        self.restarting = self.restarting and restart_options

        if restart_options and self.task_can_be_offered(TaskRestartComponents, offer):
            # We require a restart and we haven't already scheduled one.
            _log.info("Schedule a restart task")
            return self.new_task(TaskRestartComponents,
                                 restart_options=restart_options)

        # At this point we only continue when a restart is no longer required.
        if restart_options:
            # Still require a restart to complete (or fail).
            _log.info("Waiting for restart to be scheduled or to complete")
            return None

        # If necessary start Calico node and Calico libnetwork driver
        if self.task_can_be_offered(TaskRunCalicoNode, offer):
            # Calico node is not running, start it up.
            _log.info("Start Calico node")
            return self.new_task(TaskRunCalicoNode)

        if self.task_can_be_offered(TaskRunCalicoLibnetwork, offer):
            # Calico libnetwork driver is not running, start it up.
            _log.info("Start Calico libnetwork driver")
            return self.new_task(TaskRunCalicoLibnetwork)

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
        self.tasks[task.action] = task

        # Persist these tasks to the datastore.
        if self.scheduler.zk:
            self.scheduler.zk.store_tasks(self.agent_id, self.tasks)

        return task

    def task_can_be_offered(self, task_class, offer):
        """
        Whether a task can be included in the offer request.  A task can be
        included when the task type needs to be scheduled (see
        task_needs_scheduling) and the task resource requirements are fulfilled
        by the offer.
        :param task_class:
        :param offer:
        :return:
        """
        needs_scheduling = self.task_needs_scheduling(task_class)
        return needs_scheduling and task_class.can_accept_offer(offer)

    def task_needs_scheduling(self, task_class):
        """
        Whether a task needs scheduling.  A task needs scheduling if it has not
        yet been run, or if it has run and failed.  Whether a task has failed
        depends on whether the task type is persistent (i.e. always supposed to
        be running)
        :param task_class:
        :return: True if the task needs scheduling.  False otherwise.
        """
        task = self._task(task_class)
        if not task:
            return True
        if task.persistent:
            return not task.running()
        else:
            return task.failed()

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
        :param update:
        """
        # Extract the task action from the update and update the appropriate
        # task.  Updates for the restart task need special case processing
        action = Task.action_from_task_id(update.task_id.value)

        # Lookup the existing task, if there is one.
        task = self.tasks.get(action)
        if not task or task.task_id != update.task_id.value:
            _log.debug("Task is not recognised - ignoring")
            return

        _log.debug("TASK_UPDATE - %s: %s",
                mesos_pb2.TaskState.Name(update.state),
                task)

        # Update the task.
        task.update(update)

        if task.failed():
            _log.error("\t%s is in unexpected state %s with message '%s'",
                   task, mesos_pb2.TaskState.Name(update.state), update.message)
            _log.error("\tData:  %s", repr(str(update.data)))
            _log.error("\tSent by: %s", mesos_pb2.TaskStatus.Source.Name(update.source))
            _log.error("\tReason: %s", mesos_pb2.TaskStatus.Reason.Name(update.reason))
            _log.error("\tMessage: %s", update.message)
            _log.error("\tHealthy: %s", update.healthy)

        # An update to indicate a restart task is no longer running requires
        # some additional processing to re-spawn the install tasks as this
        # ensures the installation completed successfully.
        if (action == TaskRestartComponents.action) and not task.running():
            _log.debug("Handle update for restart task")
            del(self.tasks[TaskInstallNetmodules.action])
            del(self.tasks[TaskInstallDockerClusterStore.action])

        # Persist the updated tasks to the datastore.
        if self.scheduler.zk:
            self.scheduler.zk.store_tasks(self.agent_id, self.tasks)

        # If we were resyncing then check if all of our tasks are now syncd.
        if not self.agent_syncd and all(task.clean for task in self.tasks.values()):
            self.agent_syncd = True


class CalicoInstallerScheduler(mesos.interface.Scheduler):
    def __init__(self, max_concurrent_restarts=1, zk=None):
        self.agents = {}
        self.max_concurrent_restart = max_concurrent_restarts
        self.zk = zk

    def can_restart(self, agent):
        """
        Determine if we are allowed to trigger an agent restart.  We rate
        limit the number of restarts that we allow at any given time.
        :param agent:  The agent that is requesting a restart.
        :return: True if allowed, False otherwise.
        """
        if agent.restarting:
            _log.debug("Allowed to restart agent as already restarting")
            return True
        num_restarting = sum(1 for a in self.agents if a.restarting)
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
