import json

from kazoo.client import KazooClient, NoNodeError, NodeExistsError

from config import config
from tasks import TASKS_BY_CLASSNAME


class ZkDatastore(object):
    def __init__(self):
        self.zk_prefix = "root"
        self._zk = KazooClient(hosts=config.zk_hosts)
        self._zk.start()
        self._zk.ensure_path(self.agents_dir())
        self._zk.ensure_path(self.framework_id_dir())
        self.framework_id_path = self.framework_id_dir() + "/framework"

    def get_framework_id(self):
        try:
            framework_id, _ = self._zk.get(self.framework_id_path)
            return framework_id
        except NoNodeError:
            return None

    def set_framework_id(self, framework_id):
        try:
            self._zk.create(self.framework_id_path, str(framework_id))
        except NodeExistsError:
            # TODO: Should check if its the same framework id, but not sure how we'd handle that case yet
            pass

    def agents_dir(self):
        """
        Return the directory which we store agents information in.
        :return: Agents directory.
        """
        return config.zk_persist_dir + "/agent"

    def framework_id_dir(self):
        """
        Return the directory where we store framework ID in.
        :return: Framework ID directory
        """
        return config.zk_persist_dir

    def agent_path(self, agent_id):
        """
        Return the path for storing agent configuration.
        :param agent_id:  The agent ID
        :return:  The agent specific path.
        """
        return config.zk_persist_dir + "/agent/" + agent_id

    def load_tasks(self, agent_id):
        try:
            tasks_str, _stat = self._zk.get(self.agent_path(agent_id))
        except NoNodeError:
            tasks_dict = {}
        else:
            tasks_dict = json.loads(tasks_str)

        tasks = {
            cn: TASKS_BY_CLASSNAME[cn].from_dict(task_dict)
            for cn, task_dict in tasks_dict.iteritems()
        }
        return tasks

    def store_tasks(self, agent_id, tasks):
        """
        Store a group of tasks for an agent.
        :param tasks:
        :return:
        """
        tasks_dict = {
            cn: task.to_dict() for cn, task in tasks.iteritems()
        }
        tasks_json = json.dumps(tasks_dict)
        try:
            self._zk.create(self.agent_path(agent_id), tasks_json)
        except NodeExistsError:
            self._zk.set(self.agent_path(agent_id), tasks_json)

    def load_agents_raw_data(self):
        """
        Load all of the raw data for all of the agents (data is not coerced
        into Task objects.
        """
        agent_task_dicts = {}
        agent_ids = self._zk.get_children(self.agents_dir)
        for agent_id in agent_ids:
            try:
                tasks_str, _stat = self._zk.get(self.agent_path(agent_id))
            except NoNodeError:
                continue
            else:
                tasks_dict = json.loads(tasks_str)
                agent_task_dicts[agent_id] = tasks_dict
        return agent_task_dicts
