import hashlib
import logging
import uuid
from datetime import datetime

from mesos.interface import mesos_pb2

from config import config

_log = logging.getLogger(__name__)

#TODO We need a better way of handling which .so we need!
NETMODULES_SO_URL = "http://172.25.20.11/net-modules.so"

# Expected task CPUs and Mem usage
TASK_CPUS = 0.1
TASK_MEM = 128.0


class Task(object):
    hash = None
    cpus = None
    mem = None
    restarts = None
    description = None
    persistent = None

    def __init__(self, **kwargs):
        self.state = kwargs.get("state") or mesos_pb2.TASK_STAGING
        self.created = kwargs.get("created") or str(datetime.utcnow())
        self.updated = kwargs.get("updated") or "NA"
        self.task_id = kwargs.get("task_id") or self.new_task_id()
        self.hash = kwargs.get("hash") or self.hash
        self.healthy = True
        self.clean = True
        """
        Tasks loaded from the persistent store are not considered clean until
        the status has been updated from a query.
        """

        # Assert that class data has been overridden.
        assert self.hash is not None
        assert self.cpus is not None
        assert self.mem is not None
        assert self.restarts is not None
        assert self.description is not None
        assert self.persistent is not None

        # Cannot have a persistent restart task.
        assert not self.persistent or not self.restarts

    def as_new_mesos_task(self, agent_id):
        raise NotImplementedError()

    def __repr__(self):
        return "Task(%s)" % self.task_id

    def __str__(self):
        return self.__repr__()

    def new_default_task(self, agent_id):
        """
        Return a Task object containing common information for all task types.
        This is just a helper method that may be used by the as_new_mesos_task
        implementations.
        """
        task = mesos_pb2.TaskInfo()
        task.name = repr(self)
        task.task_id.value = self.task_id
        task.slave_id.value = agent_id

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem

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
        self.updated = str(datetime.utcnow())
        self.state = update.state
        self.clean = True

    def get_task_status(self, agent):
        """
        Return a TaskStatus object for this Task.
        :return: A Mesos TaskStatus instance.
        """
        task_status = mesos_pb2.TaskStatus()
        task_status.slave_id.value = agent.agent_id
        task_status.task_id.value = self.task_id
        task_status.state = self.state
        return task_status

    def to_dict(self):
        task_dict = {
            "task_id": self.task_id,
            "state": self.state,
            "created": self.created,
            "updated": self.updated,
            "hash": self.hash
        }
        return task_dict

    @classmethod
    def allowed(cls):
        """
        Whether this task is allowed.
        :return: True
        """
        return True

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
    def classname_from_task_id(cls, task_id):
        classname_suffix, _uuid = task_id.split("-", 1)
        return "Task" + classname_suffix

    @classmethod
    def new_task_id(cls):
        return cls.__name__[4:] + "-" + str(uuid.uuid4()).upper()


class TaskRunEtcdProxy(Task):
    """
    Task to run an etcd proxy.
    """
    hash = hashlib.sha256(config.etcd_binary_url).hexdigest()
    persistent = True
    restarts = False
    cpus = TASK_CPUS
    mem = TASK_MEM
    description = "Calico: etcd proxy"

    def as_new_mesos_task(self, agent_id):
        etcd_download = config.etcd_binary_url.rsplit("/", 1)[-1]
        if etcd_download.endswith(".tar.gz"):
            etcd_img = "./%s/etcd" % etcd_download[:-7]
        else:
            etcd_img = "./%s" % etcd_download

        task = self.new_default_task(agent_id)
        task.container.type = mesos_pb2.ContainerInfo.MESOS
        task.command.value = etcd_img + \
                             " --proxy=on" + \
                             " --discovery-srv=" + config.etcd_service
        task.command.user = "root"

        # Download etcd binary
        uri = task.command.uris.add()
        uri.value = config.etcd_binary_url
        uri.executable = False
        uri.cache = True
        uri.extract = True

        return task


class TaskInstallDockerClusterStore(Task):
    """
    Task to install Docker configuration for Docker multi-host networking.
    """
    hash = 0
    persistent = False
    restarts = True
    cpus = TASK_CPUS
    mem = TASK_MEM
    description = "Calico: install Docker multi-host networking"

    def as_new_mesos_task(self, agent_id):
        task = self.new_default_task(agent_id)
        task.command.value = "./installer docker"
        task.command.user = "root"

        # Download the installer binary
        uri = task.command.uris.add()
        uri.value = config.installer_url
        uri.executable = True
        uri.cache = False
        return task

    @classmethod
    def allowed(cls):
        """
        Whether this task is allowed.
        :return: True
        """
        _log.debug("Allow docker update: %s", config.allow_docker_update)
        return config.allow_docker_update


class TaskInstallNetmodules(Task):
    """
    Task to install netmodules and Calico plugin.
    """
    hash = 0
    persistent = False
    restarts = True
    cpus = TASK_CPUS
    mem = TASK_MEM
    description = "Calico: install netmodules and plugin"

    def as_new_mesos_task(self, agent_id):
        task = self.new_default_task(agent_id)
        task.command.value = "./installer netmodules"
        task.command.user = "root"

        # Download the Netmodules .so
        uri = task.command.uris.add()
        uri.value = NETMODULES_SO_URL
        uri.executable = True
        uri.cache = True
        return task

    @classmethod
    def allowed(cls):
        """
        Whether this task is allowed.
        :return: True
        """
        _log.debug("Allow agent update: %s", config.allow_agent_update)
        return config.allow_agent_update


class TaskRunCalicoNode(Task):
    """
    Task to run Calico node.
    """
    hash = hashlib.sha256(config.calicoctl_url).hexdigest()
    persistent = True
    restarts = False
    cpus = TASK_CPUS
    mem = TASK_MEM
    description = "Calico: daemon"

    def as_new_mesos_task(self, agent_id):
        cmd_ip = "$(./installer ip %s %s)" % (config.master_host,
                                              config.master_port)
        task = self.new_default_task(agent_id)
        task.command.value = "./calicoctl node --detach=false --ip=" + cmd_ip
        task.command.user = "root"

        # Add a URI for downloading the calicoctl binary
        uri = task.command.uris.add()
        uri.value = config.calicoctl_url
        uri.executable = True 
        uri.cache = True
        uri.extract = False

        # Download the installer binary
        uri = task.command.uris.add()
        uri.value = config.installer_url
        uri.executable = True
        uri.cache = True

        return task


class TaskRunCalicoLibnetwork(Task):
    """
    Task to run an Calico libnetwork.
    """
    persistent = True
    restarts = False
    cpus = TASK_CPUS
    mem = TASK_MEM
    description = "Calico: Docker networking driver"
    hash = hashlib.sha256(config.libnetwork_img).hexdigest()

    def as_new_mesos_task(self, agent_id):
        """
        Take the information stored in this Task object and fill a
        mesos task.
        """
        task = self.new_default_task(agent_id)

        task.container.type = mesos_pb2.ContainerInfo.DOCKER
        task.container.docker.image = config.libnetwork_img
        task.command.shell = False

        # Volume mount in /run/docker/plugins
        volume = task.container.volumes.add()
        volume.mode = mesos_pb2.Volume.RW
        volume.container_path = "/run/docker/plugins"
        volume.host_path = "/run/docker/plugins"

        return task
