import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from constants import VERSION, TASK_CPUS, TASK_MEM


class TaskUpdateError(Exception):
    """
    There was a problem with a TaskUpdate.
    """
    pass


class Task(object):
    def __init__(self, agent):
        self.agent = agent
        self.state = mesos_pb2.TASK_STAGING
        self.executor_id = None
        self.healthy = True

        # Construct a task ID from the task name, version and agent ID.
        self.task_id = "%s-%s-%s" % (self.name, VERSION, self.agent.agent_id)

    def __repr__(self):
        return self.task_id

    def __str__(self):
        return self.__repr__()

    def cmd(self):
        raise NotImplementedError()

    def as_new_mesos_task(self):
        """
        Take the information stored in this Task object and fill a
        mesos task.
        """
        task = mesos_pb2.TaskInfo()
        task.name = repr(self)
        task.task_id.value = self.task_id
        task.slave_id.value = self.slave_id

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
                              mesos_pb2.TASK_KILLED,
                              mesos_pb2.TASK_FAILED)

    def finished(self):
        return self.state == mesos_pb2.TASK_FINISHED

    def running(self):
        return self.state in (mesos_pb2.TASK_STAGING,
                              mesos_pb2.TASK_RUNNING)

    def update(self, update):
        self.state = update.state

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
    def agent_id_from_task_id(cls, task_id):
        _name, _version, agent_id = task_id.split("-", 2)
        return agent_id

    @classmethod
    def version_from_task_id(cls, task_id):
        _name, version, _agent_id = task_id.split("-", 2)
        return version

    @classmethod
    def name_from_task_id(cls, task_id):
        name, _version, _agent_id = task_id.split("-", 2)
        return name


class TaskRunEtcdProxy(Task):
    """
    Task to run an etcd proxy.  This is a long-running task, if the task fails
    it will be restarted.

    The task does the following:
    -  Starts an etcd proxy listening on port 2379.
    -  Performs regular health checks
    """
    name = "calico-install-etcd-proxy"
    persistent = True


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
    name = "calico-install-netmodules"
    persistent = False

    def restart_required(self):
        """
        Whether a restart is required to pick up the netmodules install.
        :return:
        """
        #TODO
        return False

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
    name = "calico-configure-docker"
    persistent = False

    def restart_required(self):
        """
        Whether a restart is required to pick up the new Docker configuration.
        :return:
        """
        #TODO
        return False


class TaskRestartComponents(Task):
    """
    Task to restart the following components:
    -  Docker daemon
    -  Mesos Agent process
    This is a short lived task.

    If the InstallDockerClusterStore task indicated that a restart is required
    then kill the Docker daemon and wait for it to restart.

    If the InstallNetmodules task indicated that a restart is required then
    kill the agent process.
    """
    name = "calico-restart-agent"
    persistent = False

    def __init__(self, agent, restart_agent=False, restart_docker=False):
        self.restart_agent = restart_agent
        self.restart_docker = restart_docker
        super(self, TaskRestartComponents).__init__(agent)


class TaskRunCalicoNode(Task):
    """
    Task to run Calico node.  This is a long-running task, if the task fails
    it will be restarted.
    """
    name = "calico-node"
    persistent = True


class TaskRunCalicoLibnetwork(Task):
    """
    Task to run an Calico libnetwork.  This is a long-running task, if the task
    fails it will be restarted.
    """
    name = "calico-libnetwork"
    persistent = True

