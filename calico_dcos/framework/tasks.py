from datetime import datetime
import uuid

from mesos.interface import mesos_pb2

from calico_dcos.common.utils import setup_logging

from calico_dcos.common.constants import (LOGFILE_FRAMEWORK,
    ACTION_RUN_ETCD_PROXY, ACTION_INSTALL_NETMODULES, ACTION_CONFIGURE_DOCKER,
    ACTION_RESTART, ACTION_CALICO_NODE, ACTION_CALICO_LIBNETWORK,
    TASK_MEM, TASK_CPUS, RESTART_COMPONENTS)

_log = setup_logging(LOGFILE_FRAMEWORK)


class Task(object):
    action = None
    version = None

    def __init__(self, **kwargs):
        self.state = kwargs.get("state") or mesos_pb2.TASK_STAGING
        self.created = kwargs.get("created") or str(datetime.utcnow())
        self.updated = kwargs.get("updated") or "NA"
        self.stdout = kwargs.get("stdout") or RESTART_COMPONENTS   #TODO Fix

        # Create a task ID if not supplied.
        assert self.action is not None, "Action has not been overridden by " \
                                        "subclass"
        self.task_id = kwargs.get("task_id") or "%s-%s" % (self.action,
                                                           uuid.uuid4())

        # Update the version from the one supplied, otherwise default to the
        # current task version.  If None, then the subclass has not overridden
        # the value.
        self.version = kwargs.get("version") or self.version
        assert self.version is not None, "Version has not been overridden " \
                                         "by subclass"

        self.healthy = True
        self.clean = True
        """
        Tasks loaded from the persistent store are not considered clean until
        the status has been updated from a query.
        """

    def __repr__(self):
        return "Task(%s)" % self.task_id

    def __str__(self):
        return self.__repr__()

    def cmd(self):
        raise NotImplementedError()

    def as_new_mesos_task(self, agent_id):
        """
        Take the information stored in this Task object and fill a
        mesos task.
        """
        task = mesos_pb2.TaskInfo()
        task.name = repr(self)
        task.task_id.value = self.task_id
        task.slave_id.value = agent_id

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        task.container.type = mesos_pb2.ContainerInfo.MESOS
        task.command.value = self.cmd()
        task.command.user = "calico"

        uri = task.command.uris.add()
        # TODO @DanO - paramaterize this through Universe
        uri.value = "http://172.25.20.11/installer"
        uri.executable = True
        uri.cache = True

        return task

    def failed(self):
        return self.state in (mesos_pb2.TASK_LOST,
                              mesos_pb2.TASK_ERROR,
                              mesos_pb2.TASK_KILLED,
                              mesos_pb2.TASK_FAILED)

    def finished(self):
        return self.state == mesos_pb2.TASK_FINISHED

    def running(self):
        return self.state in (mesos_pb2.TASK_STAGING,
                              mesos_pb2.TASK_STARTING,
                              mesos_pb2.TASK_RUNNING)

    def update(self, update):
        _log.info("Updating status of %s from %d to %d",
                  self, self.state, update.state)
        self.time_updated = str(datetime.utcnow())
        self.state = update.state
        self.clean = True
        #TODO
        #self.stdout = xxxx

    def get_task_status(self, agent):
        """
        Return a TaskStatus object for this Task.
        :return: A TaskStatus
        """
        task_status = mesos_pb2.TaskStatus()
        task_status.slave_id.value = agent.agent_id
        task_status.task_id.value = self.task_id
        task_status.state = self.state
        return task_status

    def restart_options(self):
        """
        Return the set of restart options parsed from the command output.
        :return:
        """
        # We should only be checking if the task actually finished, and if the
        # task returns a list of components to restart.
        assert self.finished()

        components = None
        for line in self.stdout.split("\n"):
            if line.startswith(RESTART_COMPONENTS):
                value = line[len(RESTART_COMPONENTS):]
                components = {c.strip() for c in value.split(",")}
                break
        if components is None:
            _log.error("Attempting to find restart components from task that "
                       "does not require restarts.")
            raise AssertionError("Not expecting restart components")
        return components

    def to_dict(self):
        task_dict = {
            "task_id": self.task_id,
            "state": self.state,
            "stdout": self.stdout,
            "created": self.created,
            "updated": self.updated,
            "version": self.version
        }
        return task_dict

    @classmethod
    def from_dict(cls, task_dict):
        # Tasks loaded from storage that were previously running are not
        # considered to be clean.  These tasks should be queried, and the
        # state updated from the query result.  If a task was not
        # previously running then it can be considered clean as the state
        # is terminal and cannot change.
        task = cls(**task_dict)
        task.clean = not task.running()
        return task

    @classmethod
    def can_accept_offer(self, offer):
        """
        Determine if this task type can accept the supplied offer or not.
        :param offer: The offer.
        :return: True if the offer contains sufficient resource to accept,
        oftherwise False.
        """
        #TODO Need to check CPU and MEM constraints.
        return True

    @classmethod
    def action_from_task_id(cls, task_id):
        action, _uuid = task_id.split("-", 1)
        return action


class TaskRunEtcdProxy(Task):
    """
    Task to run an etcd proxy.  This is a long-running task, if the task fails
    it will be restarted.

    The task does the following:
    -  Starts an etcd proxy listening on port 2379.
    -  Performs regular health checks
    """
    action = ACTION_RUN_ETCD_PROXY
    version = 0  # Increment if the run task needs to be re-executed in new
                 # release
    persistent = True

    def cmd(self):
        return "./installer %s" % self.action


class TaskInstallNetmodules(Task):
    """
    Task to install netmodules and Calico plugin.  This is short-lived task.

    The task writes to stdout the following
      restart-agent-yes
      restart-agent-no
    depending on whether a restart of the agent is required or not.

    The task does the following:
    -  Reads the /choose/a/directory/agent-start file if it exists.  It
       contains the start time of the agent process at the point the
       netmodules/calico files were installed.
       -  If the file exists and the time is different to the current time then
          return restart-agent-no.
       -  If the file exists and the time is the same as the current time then
          return restart-agent-yes.
       -  Otherwise:
          -  Installs netmodules, the calico plugin and all necessary
             configuration files.
             Note: Files are modified in an order that, should the task fail
             part way through, the agent will still be able to restart
             successfully.
          -  Write out the file /choose/a/directory/agent-start containing a
             timestamp of the agent start time.
          -  Return restart-agent-yes
    """
    action = ACTION_INSTALL_NETMODULES
    version = 0  # Increment if the run task needs to be re-executed in new
                 # release
    persistent = False

    def cmd(self):
        return "./installer %s" % self.action


class TaskInstallDockerClusterStore(Task):
    """
    Task to install Docker configuration for Docker multi-host networking.

    The task writes to stdout the following
      restart-docker-yes
      restart-docker-no
    depending on whether a restart of the docker daemon is required or not.

    This task does the following:
    -  Reads the /choose/a/directory/docker-start file if it exists.  It
       contains the start time of the docker daemon at the point the
       docker config file was installed or updated.
       -  If the file exists and the time is different to the current time then
          return restart-docker-no.
       -  If the file exists and the time is the same as the current time then
          return restart-docker-yes.
       -  Otherwise:
          -  Update the /etc/docker/daemon.json file to include the cluster
             store information.
          -  Write out the file /choose/a/directory/docker-start containing a
             timestamp of the docker daemon start time.
          -  Return restart-docker-yes
    """
    action = ACTION_CONFIGURE_DOCKER
    version = 0  # Increment if the run task needs to be re-executed in new
                 # release
    persistent = False

    def cmd(self):
        return "./installer %s" % self.action


class TaskRestartComponents(Task):
    """
    Task to restart the following components:
    -  Docker daemon
    -  Mesos Agent process
    This is a short lived task.
    """
    action = ACTION_RESTART
    version = 0  # Increment if the run task needs to be re-executed in new
                 # release
    persistent = False

    def __init__(self, **kwargs):
        self.restart_options = kwargs.get("restart_options") or set()
        super(TaskRestartComponents, self).__init__(**kwargs)

    def to_dict(self):
        task_dict = super(TaskRestartComponents, self).to_dict()
        task_dict["restart_options"] = self.restart_options
        return task_dict

    def cmd(self):
        assert self.restart_options, "Restart task should not be invoked if " \
                                     "no restarts are required"
        return "./installer %s && echo " % self.action + " ".join(self.restart_options)


class TaskRunCalicoNode(Task):
    """
    Task to run Calico node.  This is a long-running task, if the task fails
    it will be restarted.
    """
    action = ACTION_CALICO_NODE
    version = 0  # Increment if the run task needs to be re-executed in new
                 # release
    persistent = True

    def cmd(self):
        return "./installer %s " % self.action


class TaskRunCalicoLibnetwork(Task):
    """
    Task to run an Calico libnetwork.  This is a long-running task, if the task
    fails it will be restarted.
    """
    action = ACTION_CALICO_LIBNETWORK
    version = 0  # Increment if the run task needs to be re-executed in new
                 # release
    persistent = True

    def cmd(self):
        return "./installer %s " % self.action

