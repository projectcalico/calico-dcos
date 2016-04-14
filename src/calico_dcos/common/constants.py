
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