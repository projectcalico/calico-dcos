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
import logging
import os
import sys

_log = logging.getLogger(__name__)


class Config(object):
    """
    Config options for the cluster.

    Note: construction of this object happens in _main_ of this file, on import.
    Logging is not yet initialized, so print on errors.
    """
    def __init__(self):
        self._missing = []
        self.node_img = self.getenv("CALICO_NODE_IMG")
        self.allow_docker_update = self.getenv("CALICO_ALLOW_DOCKER_UPDATE", can_omit=True) != "false"
        self.enable_install_cni= self.getenv("CALICO_ENABLE_INSTALL_CNI", can_omit=True) != "false"
        self.enable_run_node = self.getenv("CALICO_ENABLE_RUN_NODE", can_omit=True) != "false"
        self.enable_run_etcd_proxy = self.getenv("CALICO_ENABLE_RUN_ETCD_PROXY", can_omit=True) != "false"
        self.max_concurrent_restarts = int(self.getenv("CALICO_MAX_CONCURRENT_RESTARTS"))
        self.zk_persist_url = self.getenv("CALICO_ZK")
        self.cpu_limit_install_docker = float(self.getenv("CALICO_CPU_LIMIT_INSTALL_DOCKER"))
        self.mem_limit_install_docker = int(self.getenv("CALICO_MEM_LIMIT_INSTALL_DOCKER"))
        self.cpu_limit_install_cni = float(self.getenv("CALICO_CPU_LIMIT_INSTALL_CNI"))
        self.mem_limit_install_cni = int(self.getenv("CALICO_MEM_LIMIT_INSTALL_CNI"))
        self.cpu_limit_etcd_proxy = float(self.getenv("CALICO_CPU_LIMIT_ETCD_PROXY"))
        self.mem_limit_etcd_proxy = int(self.getenv("CALICO_MEM_LIMIT_ETCD_PROXY"))
        self.cpu_limit_node = float(self.getenv("CALICO_CPU_LIMIT_NODE"))
        self.mem_limit_node = int(self.getenv("CALICO_MEM_LIMIT_NODE"))

        self.installer_url = self.getenv("CALICO_INSTALLER_URL")
        self.calico_cni_binary_url = self.getenv("CALICO_CNI_BINARY_URL")
        self.calico_cni_ipam_binary_url = self.getenv("CALICO_CNI_IPAM_BINARY_URL")

        self.etcd_binary_url = self.getenv("ETCD_BINARY_URL")
        self.etcd_discovery = self.getenv("ETCD_SRV")
        self.docker_cluster_store = self.getenv("DOCKER_CLUSTER_STORE", can_omit=True)
        self.etcd_endpoints = self.getenv("ETCD_ENDPOINTS")

        self.mesos_master = self.getenv("MESOS_MASTER")
        self.cni_plugins_dir = self.getenv("MESOS_CNI_PLUGINS_DIR")
        self.cni_config_dir = self.getenv("MESOS_CNI_CONFIG_DIR")

        self.webserver_bind_ip = self.getenv("LIBPROCESS_IP")
        self.webserver_bind_port = self.getenv("PORT0")
        self.webserver_dns = self.getenv("CALICO_STATUS_DNS")

        # The zk persist URL should be of the form:
        # zk://<host1:port1>,<host2:port2>.../directory/to/story/config
        assert self.zk_persist_url.startswith("zk://")
        self.zk_hosts, self.zk_persist_dir = self.zk_persist_url[5:].split("/", 1)

        if not self.docker_cluster_store:
            """
            If user hasn't specified a cluster_store, convert the ETCD_ENDPOINTS into a valid docker cluster store.
            example:

            'http://m1.dcos:2379,http://m2.dcos:2379,http://m3.dcos:2379'
            to
            'etcd://m1.dcos:2379,m2.dcos:2379,m3.dcos:2379'
            """
            self.docker_cluster_store = "etcd://"
            cluster_store_without_uri = []
            for endpoint in self.etcd_endpoints.split(","):
                if endpoint[:7] != "http://":
                    _log.error("Calico Installation Framework currently only supports http. "
                               "If you'd like to use https, please manually install Calico in DC/OS.")
                    sys.exit(1)
                cluster_store_without_uri.append(endpoint[7:])
            self.docker_cluster_store += ",".join(cluster_store_without_uri)
            print "No cluster store provided. Using: %s" % self.docker_cluster_store

        # Construct the web status URL
        self.webserver_url = "http://%s:%s/" % (self.webserver_dns,
                                                self.webserver_bind_port)

        # Trace out the parsed environment variables.
        for key, val in self.__dict__.iteritems():
            if key.startswith("_"):
                continue
            _log.debug("%s = %s", key, val)

        # If we are missing environment variables, trace them out now.
        if self._missing:
            # Print instead of logging, since logging hasn't been initialized
            print "Missing environment variables: %s" % self._missing
            print "Current environment variables: %s" % os.environ
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
