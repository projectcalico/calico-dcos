
TASK_CPUS = 0.1
TASK_MEM = 128.0

# Logfiles used by the framework and installer script
LOGFILE_INSTALLER = '/var/log/calico/calico_installer.log'
LOGFILE_FRAMEWORK = '/var/log/calico/calico_framework.log'

# Version of the installer framework
VERSION = "1.0.0"

# Installation script actions
ACTION_RUN_ETCD_PROXY = "calico_install_etcd_proxy"
ACTION_INSTALL_NETMODULES = "calico_install_netmodules"
ACTION_CONFIGURE_DOCKER = "calico_configure_docker"
ACTION_RESTART = "calico_restart_agent"
ACTION_CALICO_NODE = "calico_node"
ACTION_CALICO_LIBNETWORK = "calico_libnetwork"

# Prefix of line containing a list of components to restart
RESTART_COMPONENTS = "Restart components: "

# Return values from the installation script.
RESTART_DOCKER = "restart-docker"
RESTART_AGENT = "restart-agent"

# Max time for process restarts (in seconds)
MAX_TIME_FOR_DOCKER_RESTART = 30