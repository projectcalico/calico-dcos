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
import logging

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

from config import config
from tasks import Task, TASK_ORDER
from webserver import launch_webserver
from zookeeper import ZkDatastore


_log = logging.getLogger(__name__)


# TODO
# o  Can we determine what slave the framework is running on?  That would help limit the issues
#    caused by restart - worst case scenario is we keep restarting the node that runs the framework...
#    kill one agent, framework moves to another, then we next that agent...and so on.  Shouldn't actually
#    matter, but it would slow things down.

# Instantiate a ZooKeeper instance for use by the Framework.
zk = ZkDatastore()


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
        -  Then Calico node can be started.

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
        _log.debug("Handling offer from agent %s", offer.slave_id.value[-8:])

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
                _log.debug("Task is not allowed - skipping")
                continue

            if self.task_needs_scheduling(task_class):
                _log.debug("Task needs scheduling")
                if self.task_can_be_offered(task_class, offer):
                    _log.debug("Task can be offered")
                    role = self.get_role(offer)
                    return self.new_task(task_class, role=role)
                else:
                    _log.debug("Task cannot be offered - wait")
                    return None

            if self.task_in_progress(task_class):
                _log.debug("Waiting for task to complete")
                return None

        _log.debug("All tasks are running")
        return None

    def get_role(self, offer):
        """
        Helper method to disect an offer and determine which role the resources
        belong to.

        Note: This method (and this framework) make the following assumptions
        about role assignments in DC/OS:
        - Only 2 roles exist: "*", and "public_slave"
        - A (public) slave offering "public_slave" resources will not offer enough "*"
        resources to ever launch a task.

        :return: A string representation of the role. This can be "*" or a string.
        """
        # If we find anything besides "*", use it as the role. Otherwise, just use "*"
        for resource in offer.resources:
            if resource.role != "*":
                return resource.role
        return "*"

    def trigger_resync(self, driver):
        """
        Trigger a resync of the tasks persisted in ZooKeeper.
        :param driver:
        :return:
        """
        # Query the state of each task that was previously running.  Terminated
        # tasks do not need to be queried because their state won't have
        # changed, and the task may have been tidied up.
        self.tasks = zk.load_tasks(self.agent_id)
        task_statuses = [task.get_task_status(self)
                         for task in self.tasks.itervalues() if task.running()]

        if not task_statuses:
            # No tasks stored.  Although we can assume we are sync'd, we still
            # perform a full reconcile so that we can terminate any old tasks
            # that the framework may own as they may otherwise prevent the new
            # tasks from operating correctly.
            _log.info("No running tasks, assume sync'd")
            self.agent_syncd = True
            driver.reconcileTasks([])
        else:
            # Start the resync and indicate that it is not yet complete.
            _log.debug("Triggering a reconcile.")
            self.agent_syncd = False
            driver.reconcileTasks(task_statuses)

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
        zk.store_tasks(self.agent_id, self.tasks)

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
            _log.debug("Schedule task: Task '%s' has not run yet.", task_class)
            return True

        if task.hash != task_class.hash:
            _log.debug("Schedule task: Task '%s' has been changed.  Prev=%s, Curr=%s",
                       task_class, task.hash, task_class.hash)
            return True

        if task.failed():
            _log.debug("Schedule task: Task '%s' is in a failed state.", task_class)
            return True

        if task.persistent and not task.running():
            _log.debug("Schedule task: Task '%s' is persistent but is not currently running.", task_class)
            return True

        _log.debug("Task '%s' does not need scheduling", task_class)
        return False

    def task_in_progress(self, task_class):
        """
        A task is in progress if it is running and the task is not persistent.
        :param task_class:
        :return:
        """
        task = self._task(task_class)
        if not task:
            return False
        return task.running() and not task.persistent

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
            if update.state == mesos_pb2.TASK_RUNNING:
                _log.debug("Killing unrecognized task: %s" % update.task_id.value)
                driver.killTask(update.task_id)
            else:
                _log.debug("Ignoring update from unrecognized stopped task.")
            return
        _log.debug("TASK_UPDATE - %s: %s on Slave %s",
                   mesos_pb2.TaskState.Name(update.state),
                   task,
                   update.slave_id.value[-7:])

        # Update the task.
        task.update(update)

        # We expect restart tasks to fail, but not others.
        if task.failed() and not task.restarts:
            _log.error("\t%s is in unexpected state %s with message '%s'",
                   task, mesos_pb2.TaskState.Name(update.state), update.message)
            _log.debug("\tData:  %s", repr(str(update.data)))
            _log.debug("\tSent by: %s", mesos_pb2.TaskStatus.Source.Name(update.source))
            _log.debug("\tReason: %s", mesos_pb2.TaskStatus.Reason.Name(update.reason))
            _log.debug("\tMessage: %s", update.message)
            _log.debug("\tHealthy: %s", update.healthy)

        # Persist the updated tasks to the datastore.
        zk.store_tasks(self.agent_id, self.tasks)

        # If we were resyncing then check if all of our tasks are now syncd.
        if not self.agent_syncd and all(task.clean for task in self.tasks.values()):
            _log.debug("Agent is now sync'd - perform full reconcile to tidy "
                       "up old tasks")
            self.agent_syncd = True
            driver.reconcileTasks([])

    def is_restarting(self):
        """
        Return whether a component restart is in progress.  A restart is in
        progress when a restart task is not finished.
        :return: True if any restart task is in progress.
        """
        restarting = any(t.restarts and not t.finished() for t in self.tasks.values())
        _log.debug("Agent %s is restarting: %s", self.agent_id, restarting)
        return restarting


class CalicoInstallerScheduler(mesos.interface.Scheduler):
    def __init__(self):
        self.agents = {}

    def can_restart_agent(self, agent):
        """
        Determine if we are allowed to trigger an agent restart.

        We only allow a maximum number of agents to be restarted at the same
        time.  If the requesting agent is already restarting then it is
        allowed to restart.

        :param agent:
        :return: True if the agent can be restarted, False otherwise.
        """
        if agent.is_restarting():
            _log.debug("Agent is already restarting - allowed to restart")
            return True
        num_restarting = sum(1 for a in self.agents.values() if a.is_restarting())
        can_restart = num_restarting < config.max_concurrent_restarts
        _log.debug("Can restart agent: %s", can_restart)
        return can_restart

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
        zk.set_framework_id(frameworkId.value)

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
        agent = self.get_agent(slave_id.value)
        agent.agent_syncd = None

    def offerRescinded(self, driver, offer_id):
        # Not obvious if this is actually required.  We probably need to track
        # offer to speed things up, but I don't believe it's strictly necessary.
        _log.info("Offer rescinded for offer ID %s", offer_id)

    def error(self, driver, message):
        """
        Invoked when there is an unrecoverable error in the scheduler.
        :param driver:
        :param message:
        :return:
        """
        _log.error("Encountered error: %s", message)
        if message == "Framework has been removed":
            _log.error("Error: Unable to register with the framework ID stored in zk."
                       "This commonly happens when the Calico package is removed and then re-added."
                       "Wiping calico's ZK data so the next restart can get a new ID.")
            zk.remove_calico()

def launch_framework():
    """
    Launch the Calico framework.
    :return: The Mesos driver.  The caller should call driver.join() to ensure
    thread is blocked until driver exits.
    """
    _log.info("Connecting to Master: %s", config.mesos_master)
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "root"
    framework.name = "calico"
    framework.principal = "calico"
    framework.failover_timeout = 604800
    framework.role = "slave_public"
    framework.webui_url = config.webserver_url

    old_id = zk.get_framework_id()
    if old_id:
        _log.info("Using old framework ID: %s", old_id)
        framework.id.value = old_id


    _log.info("Launching Calico Mesos scheduler")
    scheduler = CalicoInstallerScheduler()
    driver = mesos.native.MesosSchedulerDriver(scheduler,
                                               framework,
                                               config.mesos_master)
    driver.start()
    return driver


def initialise_logging():
    """
    Initialise logging to stdout.
    """
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s]\t%(name)s %(lineno)d: %(message)s')
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Reduce the kazoo logging as it is somewhat chatty.
    logging.getLogger('kazoo.client').setLevel(logging.INFO)


if __name__ == "__main__":
    initialise_logging()
    driver_thread = launch_framework()
    webserver_thread = launch_webserver()

    # We only join the driver thread, since we want to terminate the framework
    # if the driver dies - in which case Marathon will re-launch.  If the
    # webserver dies (which is less important), Marathon will fail health
    # checks and relaunch the framework.
    driver_thread.join()
