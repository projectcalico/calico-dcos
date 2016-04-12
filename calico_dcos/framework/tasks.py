import uuid

from mesos.interface import mesos_pb2

from calico_dcos.common.utils import setup_logging

from calico_dcos.common.constants import (VERSION, LOGFILE_FRAMEWORK,
    ACTION_RUN_ETCD_PROXY, ACTION_INSTALL_NETMODULES, ACTION_CONFIGURE_DOCKER,
    ACTION_RESTART, ACTION_CALICO_NODE, ACTION_CALICO_LIBNETWORK,
    TASK_MEM, TASK_CPUS, RESTART_COMPONENTS)

_log = setup_logging(LOGFILE_FRAMEWORK)


class Task(object):
    def __init__(self, task_id=None, state=mesos_pb2.TASK_STAGING, stdout=""):
        self.state = state
        self.executor_id = None
        self.healthy = True
        self.stdout = RESTART_COMPONENTS

        # Create a task ID if not supplied.
        self.task_id = task_id or "%s-%s-%s" % (self.action,
                                                VERSION,
                                                uuid.uuid4())

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
            "stdout": self.stdout
        }
        return task_dict

    @classmethod
    def from_dict(cls, task_dict):
        task = cls(task_id=task_dict["task_id"],
                   state=task_dict["state"])

        # Tasks loaded from storage that were previously running are not
        # considered to be clean.  These tasks should be queried, and the
        # state updated from the query result.  If a task was not
        # previously running then it can be considered clean as the state
        # is terminal and cannot change.
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
    def version_from_task_id(cls, task_id):
        _action, version, _uuid = task_id.split("-", 2)
        return version

    @classmethod
    def action_from_task_id(cls, task_id):
        action, _version, _uuid = task_id.split("-", 2)
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
    persistent = True

    def cmd(self):
        return "ip addr && sleep 30"


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
    persistent = False

    def cmd(self):
        return "ip addr"


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
    persistent = False

    def cmd(self):
        return "ip addr"


class TaskRestartComponents(Task):
    """
    Task to restart the following components:
    -  Docker daemon
    -  Mesos Agent process
    This is a short lived task.
    """
    action = ACTION_RESTART
    persistent = False

    def __init__(self, task_id=None, state=mesos_pb2.TASK_STAGING,
                 stdout="", restart_options=None):
        self.restart_options = restart_options or set()
        super(self, TaskRestartComponents).__init__(task_id=task_id,
                                                    state=state,
                                                    stdout=stdout)

    def cmd(self):
        assert self.restart_options, "Restart task should not be invoked if " \
                                     "no restarts are required"
        return "ip addr && echo " + " ".join(self.restart_options)


class TaskRunCalicoNode(Task):
    """
    Task to run Calico node.  This is a long-running task, if the task fails
    it will be restarted.
    """
    action = ACTION_CALICO_NODE
    persistent = True

    def cmd(self):
        return "ip addr && sleep 30"


class TaskRunCalicoLibnetwork(Task):
    """
    Task to run an Calico libnetwork.  This is a long-running task, if the task
    fails it will be restarted.
    """
    action = ACTION_CALICO_LIBNETWORK
    persistent = True

    def cmd(self):
        return "ip addr && sleep 30"

