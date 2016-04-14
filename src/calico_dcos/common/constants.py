
TASK_CPUS = 0.1
TASK_MEM = 128.0

CALICOCTL_BINARY_URL = "https://github.com/projectcalico/calico-containers/releases/download/v0.18.0/calicoctl"
ETCD_BINARY_URL = "https://github.com/coreos/etcd/releases/download/v2.3.1/etcd-v2.3.1-linux-amd64.tar.gz"
INSTALLER_URL = "http://172.25.20.11/installer"
NETMODULES_SO_URL = "http://172.25.20.11/net-modules.so"

# @@DJO TODO: paramaterize master.mesos and port
MESOS_MASTER_HOSTNAME = "master.mesos"
MESOS_MASTER_PORT = 5050

# Logfiles used by the framework and installer script
LOGFILE_INSTALLER = './calico_installer.log'
LOGFILE_FRAMEWORK = '/var/log/calico/calico_framework.log'

# Installation script actions
ACTION_RUN_ETCD_PROXY = "calico_install_etcd_proxy"
ACTION_INSTALL_NETMODULES = "calico_install_netmodules"
ACTION_CONFIGURE_DOCKER = "calico_configure_docker"
ACTION_RESTART = "calico_restart_agent"
ACTION_GET_AGENT_IP = "calico_get_agent_ip"
ACTION_CALICO_NODE = "calico_node"
ACTION_CALICO_LIBNETWORK = "calico_libnetwork"

# Prefix of line containing a list of components to restart
RESTART_COMPONENTS = "Restart components: "

# Return values from the installation script.
RESTART_DOCKER = "restart-docker"
RESTART_AGENT = "restart-agent"

