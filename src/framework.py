import os

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

from calico_dcos.framework.framework import ZkDatastore, CalicoInstallerScheduler, _log

if __name__ == "__main__":
    # Extract relevant configuration from our environment
    master_ip = os.getenv('MESOS_MASTER', 'mesos.master')
    max_concurrent_restarts = os.getenv('MAX_CONCURRENT_RESTARTS', 1)
    zk_persist_url = os.getenv('ZK_PERSIST')
    etcd_service = os.getenv('ETCD_SERVICE_ADDR')

    _log.info("Connecting to Master: %s", master_ip)
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "root"  # Have Mesos fill in the current user.
    framework.name = "Calico framework"
    framework.principal = "calico-framework"
    framework.id.value = "calico-framework"
    framework.failover_timeout = 604800

    _log.info("Using zk: %s", zk_persist_url)
    zk = ZkDatastore(zk_persist_url)

    scheduler = CalicoInstallerScheduler(
        max_concurrent_restarts=max_concurrent_restarts, zk=zk)

    _log.info("Launching Calico Mesos scheduler")
    driver = mesos.native.MesosSchedulerDriver(scheduler,
                                               framework,
                                               master_ip)
    driver.start()
    driver.join()
