import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from constants import BAD_TASK_STATES, UNFINISHED_TASK_STATES
from constants import TASK_CPUS, TASK_MEM


class TaskUpdateError(Exception):
    """
    There was a problem with a TaskUpdate.
    """
    pass


class Task(object):
    def __init__(self, slave=None, *args, **kwargs):

        self.slave = slave
        self.state = None
        self.task_id = None
        self.executor_id = None
        self.slave_id = None

    def as_new_mesos_task(self):
        """
        Take the information stored in this Task object and fill a
        mesos task.
        """
        assert self.task_id, "Calico task must be assigned a task_id"
        assert self.slave_id, "Calico task must be assigned a slave_id"

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

        return task

    def process_update(self, update):
        # Update the calico_task with info from TaskInfo
        self.state = update.state


class SleepTask(Task):
    def __repr__(self):
        return "SleepTask(id=%s)" % (self.task_id)


    def as_new_mesos_task(self):
        """
        Extends the basic mesos task settings by adding  a custom label called "task_type" which the executor will
        read to identify the task type.
        """
        task = super(SleepTask, self).as_new_mesos_task()
        # KEEP THIS: each task sets their command here
        task.command.value = "sleep 20"
        return task
