import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TEST_TIMEOUT = 45
TASK_CPUS = 0.1
TASK_MEM = 128.0
LOGFILE = '/var/log/calico/calico_installer.log'

COMPLETED_OK_STATES = [mesos_pb2.TASK_COMPLETED]
COMPLETED_ERROR_STATES = [mesos_pb2.TASK_LOST, mesos_pb2.TASK_KILLED, mesos_pb2.TASK_FAILED]
UNFINISHED_TASK_STATES = [mesos_pb2.TASK_RUNNING, mesos_pb2.TASK_STAGING]

# Version of the installer framework
VERSION = "1.0.0"