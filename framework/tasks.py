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
import hashlib
import logging
import uuid
import json
from datetime import datetime

from mesos.interface import mesos_pb2

from config import config, USE_CACHED_INSTALLER

_log = logging.getLogger(__name__)


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
        self.role = kwargs.get("role") or "*"
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
        cpus.role = self.role

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem
        mem.role = self.role

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
        _log.info("Updating status of %s from %s to %s",
                  self,
                  mesos_pb2.TaskState.Name(self.state),
                  mesos_pb2.TaskState.Name(update.state))
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
            "hash": self.hash,
            "role": self.role
        }
        return task_dict

    @classmethod
    def allowed(cls):
        """
        Whether this task is allowed.  A class may override this if there are
        configuration options that may be used to disallow the step.
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
    def can_accept_offer(cls, offer):
        """
        Determine if this task type can accept the supplied offer or not.
        :param offer: The offer.
        :return: True if the offer contains sufficient resource to accept,
        oftherwise False.
        """
        cpus = 0.0
        mem = 0.0
        for resource in offer.resources:
            if resource.name == "cpus":
                cpus += resource.scalar.value
            elif resource.name == "mem":
                mem += resource.scalar.value
        can_accept = cpus >= cls.cpus and mem >= cls.mem
        _log.debug("Offer resources:  mem=%s, cpus=%s", mem, cpus)
        _log.debug("Required resources:  mem=%s, cpus=%s", cls.mem, cls.cpus)
        _log.info("Can accept offer: %s", can_accept)
        return can_accept

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
    cpus = config.cpu_limit_etcd_proxy
    mem = config.mem_limit_etcd_proxy
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
                             " --discovery-srv=" + config.etcd_discovery
        task.command.user = "root"

        # Download etcd binary
        uri = task.command.uris.add()
        uri.value = config.etcd_binary_url
        uri.executable = False
        uri.cache = True
        uri.extract = True

        return task

    @classmethod
    def allowed(cls):
        _log.debug("Allow run etcd proxy: %s", config.enable_run_etcd_proxy)
        return config.enable_run_etcd_proxy


class TaskInstallDockerClusterStore(Task):
    """
    Task to install Docker configuration for Docker multi-host networking.
    """
    hash = 0
    persistent = False
    restarts = True
    cpus = config.cpu_limit_install_docker
    mem = config.mem_limit_install_docker
    description = "Calico: install Docker multi-host networking"

    def as_new_mesos_task(self, agent_id):
        task = self.new_default_task(agent_id)
        task.command.value = "./installer docker %s" % config.docker_cluster_store
        task.command.user = "root"

        # Download the installer binary
        uri = task.command.uris.add()
        uri.value = config.installer_url
        uri.executable = True
        uri.cache = USE_CACHED_INSTALLER
        return task

    @classmethod
    def allowed(cls):
        """
        Whether this task is allowed.  There is a configuration option to
        prevent a Docker restart - in which case this step should be skipped.
        :return: True
        """
        _log.debug("Allow docker update: %s", config.allow_docker_update)
        return config.allow_docker_update


class TaskInstallCalicoCNI(Task):
    """
    Task to install netmodules and Calico plugin.
    """
    hash = 0
    persistent = False
    restarts = True
    cpus = config.cpu_limit_install_cni
    mem = config.mem_limit_install_cni
    description = "Calico: install CNI plugin"

    def as_new_mesos_task(self, agent_id):
        task = self.new_default_task(agent_id)
        task.command.user = "root"
        task.command.value = "./installer cni %s %s %s" % (config.cni_plugins_dir, config.cni_config_dir, config.etcd_endpoints)
        if self.role == "slave_public":
            task.command.value += " --public"

        # Download the Calico CNI Binary
        uri = task.command.uris.add()
        uri.value = config.calico_cni_binary_url
        uri.executable = True
        uri.cache = True
        uri.extract = False

        # Download Calico CNI IPAM Binary
        uri = task.command.uris.add()
        uri.value = config.calico_cni_ipam_binary_url
        uri.executable = True
        uri.cache = True
        uri.extract = False

        # Download the installer binary
        uri = task.command.uris.add()
        uri.value = config.installer_url
        uri.executable = True
        uri.cache = USE_CACHED_INSTALLER

        return task

    @classmethod
    def allowed(cls):
        return config.enable_install_cni


class TaskRunCalicoNode(Task):
    """
    Task to run Calico node.
    """
    hash = 0
    persistent = True
    restarts = False
    cpus = config.cpu_limit_node
    mem = config.mem_limit_node
    description = "Calico: daemon"

    def as_new_mesos_task(self, agent_id):
        cmd_ip = "$(./installer ip %s)" % config.zk_hosts
        task = self.new_default_task(agent_id)

        task.command.value = "docker rm -f calico-node | true && " \
                             "docker run " \
                             "--restart=always " \
                             "--net=host " \
                             "--privileged " \
                             "--name=calico-node " \
                             "-e FELIX_IGNORELOOSERPF=true " \
                             "-v /lib/modules:/lib/modules " \
                             "-v /var/run/calico:/var/run/calico " \
                             "-v /run/docker/plugins:/run/docker/plugins " \
                             "-v /var/run/docker.sock:/var/run/docker.sock " \
                             "-e CALICO_LIBNETWORK_ENABLED=true " \
                             "-e IP=%s " \
                             "-e HOSTNAME=$(hostname) " \
                             "-e ETCD_ENDPOINTS=%s " \
                             "-e ETCD_SCHEME=http " \
                             "%s " % (cmd_ip, config.etcd_endpoints, config.node_img)
        task.command.user = "root"

        # Download the installer binary
        uri = task.command.uris.add()
        uri.value = config.installer_url
        uri.executable = True
        uri.cache = USE_CACHED_INSTALLER

        return task

    @classmethod
    def allowed(cls):
        return config.enable_run_node

# Define the task order, and create a mapping between task name and classname
# because we use the latter to convert a task ID to a task class.
TASK_ORDER = [
    TaskRunEtcdProxy,
    TaskInstallDockerClusterStore,
    TaskInstallCalicoCNI,
    TaskRunCalicoNode
]
TASKS_BY_CLASSNAME = {cls.__name__: cls for cls in TASK_ORDER}

