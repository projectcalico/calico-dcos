import logging
import os
import sys

_log = logging.getLogger(__name__)


class Config(object):
    def __init__(self):
        self._missing = []
        self.calicoctl_url = self.getenv("CALICO_CALICOCTL_URL")
        self.node_img = self.getenv("CALICO_NODE_IMG")
        self.libnetwork_img = self.getenv("CALICO_LIBNETWORK_IMG")
        self.allow_docker_update = bool(self.getenv("CALICO_ALLOW_DOCKER_UPDATE", can_omit=True))
        self.allow_agent_update = bool(self.getenv("CALICO_ALLOW_AGENT_UPDATE", can_omit=True))
        self.max_concurrent_restarts = int(self.getenv("CALICO_MAX_CONCURRENT_RESTARTS"))
        self.zk_persist_url = self.getenv("CALICO_ZK")
        self.cpu_limit_install = float(self.getenv("CALICO_CPU_LIMIT_INSTALL"))
        self.mem_limit_install = int(self.getenv("CALICO_MEM_LIMIT_INSTALL"))
        self.cpu_limit_etcd_proxy = float(self.getenv("CALICO_CPU_LIMIT_ETCD_PROXY"))
        self.mem_limit_etcd_proxy = int(self.getenv("CALICO_MEM_LIMIT_ETCD_PROXY"))
        self.cpu_limit_node = float(self.getenv("CALICO_CPU_LIMIT_NODE"))
        self.mem_limit_node = int(self.getenv("CALICO_MEM_LIMIT_NODE"))
        self.cpu_limit_libnetwork = float(self.getenv("CALICO_CPU_LIMIT_LIBNETWORK"))
        self.mem_limit_libnetwork = int(self.getenv("CALICO_MEM_LIMIT_LIBNETWORK"))

        self.installer_url = self.getenv("CALICO_INSTALLER_URL")
        self.calico_mesos_url = self.getenv("CALICO_MESOS_PLUGIN")

        self.etcd_binary_url = self.getenv("ETCD_BINARY_URL")
        self.etcd_discovery = self.getenv("ETCD_SRV")

        self.mesos_master = self.getenv("MESOS_MASTER")

        # The zk persist URL should be of the form:
        # zk://<host1:port1>,<host2:port2>.../directory/to/story/config
        assert self.zk_persist_url.startswith("zk://")
        self.zk_hosts, self.zk_persist_dir = self.zk_persist_url[5:].split("/", 1)

        # Trace out the environment variables.
        for key, val in self.__dict__:
            if key.startswith("_"):
                continue
            _log.debug("%s = %s", key, val)

        # If we are missing environment variables, trace them out now.
        if self._missing:
            # Print instead of logging, since logging hasn't been initialized
            print "Missing environment variables: %s" % self._missing
            sys.exit(1)

    def getenv(self, key, can_omit=False):
        value = os.getenv(key)
        if value is None and not can_omit:
            self._missing.append(key)
        return value

# Instantiate a configuration class
config = Config()



# Development changes to the installer won't be picked up unless
# the binary uses a new name, or cache is disabled.
# Set this flag to False if you are updating the installer
# at CALICO_INSTALLER_URL without changing its name.
USE_CACHED_INSTALLER = False
