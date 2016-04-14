import json
import os
import shutil
import socket
import sys
import time
from collections import OrderedDict

import psutil
from calico_dcos.common.constants import (LOGFILE_INSTALLER,
    ACTION_INSTALL_NETMODULES, ACTION_CONFIGURE_DOCKER, ACTION_RESTART,
    ACTION_CALICO_NODE, RESTART_DOCKER, RESTART_AGENT, RESTART_COMPONENTS,
    MAX_TIME_FOR_DOCKER_RESTART, ACTION_GET_AGENT_IP,
    MESOS_MASTER_HOSTNAME, MESOS_MASTER_PORT)
from calico_dcos.common.version import VERSION

from src.calico_dcos.common.utils import setup_logging

_log = setup_logging(LOGFILE_INSTALLER)

# Calico installation config files
INSTALLER_CONFIG_DIR = "/etc/calico/installer"
NETMODULES_INSTALL_CONFIG= INSTALLER_CONFIG_DIR + "/netmodules"
DOCKER_INSTALL_CONFIG = INSTALLER_CONFIG_DIR + "/docker"

# Docker information for a standard Docker install.
DOCKER_EXE = "/usr/bin/docker"
DOCKER_DAEMON_PARMS = ["daemon"]
DOCKER_DAEMON_CONFIG = "/etc/docker/daemon.json"

# Docker information for a standard Docker install.
AGENT_CONFIG = "/opt/mesosphere/etc/mesos-slave-common"
AGENT_EXE = "/usr/bin/docker"
AGENT_PARMS = ["daemon"]
AGENT_MODULES_CONFIG = "/opt/mesosphere/etc/mesos-slave-modules.json"

# Fixed address for our etcd proxy.
CLUSTER_STORE_ETCD_PROXY = "etcd://127.0.0.1:2379"

# Max time for process restarts (in seconds)
MAX_TIME_FOR_DOCKER_RESTART = 30
MAX_TIME_FOR_AGENT_RESTART = 30

def ensure_dir(directory):
    """
    Ensure the specified directory exists
    :param directory:
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def find_process(exe, parms):
    """
    Find the unique process specified by the executable and command line
    regexes.

    :param exe:
    :param parms:
    :return:
    """
    processes = [
        p for p in psutil.process_iter()
        if p.exe() == exe and all(parm in p.cmdline() for parm in parms)
    ]

    # Multiple processes suggests our query is not correct.
    if len(processes) > 1:
        _log.error("Found multiple matching processes: %s", exe)
        sys.exit(1)

    return processes[0]


def wait_for_process(exe, parms, max_wait):
    """
    Locate the specified process, waiting a specified max time before giving
    up.  If the process can not be found within the time limit, the script
    exits.
    :param exe:
    :param parms:
    :param max_wait:
    :return:  The located process.
    """
    start = time.time()
    process = find_process(exe, parms)
    while not process and time.time() < start + max_wait:
        time.sleep(1)
        process = find_process(exe, parms)

    if not process:
        _log.error("Process not found within timeout: %s", exe)
        sys.exit(1)

    return process


def load_config(filename):
    """
    Load a JSON config file from disk.
    :param filename:  The filename of the config file
    :return:  A dictionary containing the JSON data.  If the file was not found
    an empty dictionary is returned.
    """
    if not os.path.exists(filename):
        return {}
    with open(filename) as f:
        return json.loads(f.read())


def store_config(filename, config):
    """
    Store the supplied config as a JSON file.  This performs an atomic write
    to the config file by writing to a temporary file and then renaming the
    file.
    :param filename:  The filename
    :param config:  The config (a simple dictionary)
    """
    ensure_dir(os.path.dirname(filename))
    atomic_write(json.dumps(config))


def load_property_file(filename):
    """
    Loads a file containing x=a,b,c... properties separated by newlines, and
    returns an OrderedDict where the key is x and the value is [a,b,c...]
    :param filename:
    :return:
    """
    props = OrderedDict()
    if not os.path.exists(filename):
        return props
    with open(filename) as f:
        for line in f:
            line = line.strip().split("=", 1)
            if len(line) != 2:
                continue
            props[line[0].strip()] = line[2].split(",")
    return props


def store_property_file(filename, props):
    """
    Write a property file (see load_property_file)
    :param filename:
    :return:
    """
    config = "\n".join(prop + "=" + ",".join(vals)
                       for prop, vals in props.iteritems())
    atomic_write(filename, config)


def atomic_write(filename, contents):
    """
    Atomic write a file, by first writing out a temporary file and then
    moving into place.  The temporary filename is simply the supplied
    filename with ".tmp" appended.
    :param filename:
    :param contents:
    """
    ensure_dir(os.path.dirname(filename))
    tmp = filename + ".tmp"
    with open(tmp, "w") as f:
        f.write(contents)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, filename)


def copy_file(from_file, to_file):
    """
    Checks if the file exists and if not moves it into place.
    :return: Whether the file was moved.
    """
    if os.path.exists(to_file):
        return False
    ensure_dir(os.path.dirname(to_file))
    shutil.copy(from_file, to_file)
    return True


def output_restart_components(components):
    """
    Output to stdout which components need restarting.
    :param components:
    """
    print RESTART_COMPONENTS + ",".join(components or [])


def cmd_install_netmodules():
    """
    Install netmodules and Calico plugin.  This command does not block.

    The command writes to stdout the following
      restart-agent-yes
      restart-agent-no
    depending on whether a restart of the agent is required or not.

    The command does the following:
    -  Reads the /etc/calico/installer/agent-start file if it exists.  It
       contains the start time of the agent process at the point the
       netmodules/calico files were installed.
       -  If the file exists and the time is different to the current time then
          return restart-agent-no.
       -  If the file exists and the time is the same as the current time then
          return restart-agent-yes.
       -  Otherwise:
          -  Installs netmodules, the calico plugin and all necessary
             configuration files.
             Note: Files are modified in an order that, should the task fail
             part way through, the agent will still be able to restart
             successfully.
          -  Write out the file /etc/calico/installer/agent-start containing a
             timestamp of the agent start time.
          -  Return restart-agent-yes

    :return:
    """
    # Load the current Calico install info for Docker, and the current Docker
    # daemon configuration.
    install_config = load_config(NETMODULES_INSTALL_CONFIG)
    modules_config = load_config(AGENT_MODULES_CONFIG)
    mesos_props = load_property_file(AGENT_CONFIG)

    hooks = mesos_props.get("MESOS_HOOKS", [])
    if "com_mesosphere_mesos_NetworkHook" not in hooks:
        # Flag that config is updated and reset the create time, then update
        # the config and copy the files.  Make sure the last thing we do is
        # update the config that we check above.
        _log.debug("Configure netmodules and calico in Mesos")
        install_config["netmodules-updated"] = True
        install_config["netmodules-created"] = None
        store_config(NETMODULES_INSTALL_CONFIG, install_config)

        # Copy the netmodules .so and the calico binary.
        copy_file("./libmesos_network_isolator.so",
                  "/opt/mesosphere/lib/mesos/libmesos_network_isolator.s")
        copy_file("./calico_mesos",
                  "/calico/calico_mesos")

        # Update the modules config to reference the .so
        modules_update = {
          "libraries": [
            {
              "file": "/opt/mesosphere/lib/libmesos_network_isolator.so",
              "modules": [
                {
                  "name": "com_mesosphere_mesos_NetworkIsolator",
                  "parameters": [
                    {
                      "key": "isolator_command",
                      "value": "/calico/calico_mesos"
                    },
                    {
                      "key": "ipam_command",
                      "value": "/calico/calico_mesos"
                    }
                  ]
                },
                {
                  "name": "com_mesosphere_mesos_NetworkHook"
                }
              ]
            }
          ]
        }
        modules_config.update(modules_update)
        store_config(AGENT_MODULES_CONFIG, modules_config)

        # Finally update the properties.  We do this last, because this is what
        # we check to see if files are copied into place.
        mesos_props["MESOS_ISOLATION"].append("com_mesosphere_mesos_NetworkHook")
        mesos_props["MESOS_HOOKS"] = ["com_mesosphere_mesos_NetworkHook"]
        store_property_file(AGENT_CONFIG, mesos_props)

    if not install_config.get("netmodules-updated"):
        _log.debug("NetworkHook not updated by Calico")
        return None

    if not install_config.get("netmodules-created"):
        # Mesos configuration was updated, store the current agent process
        # ID and then indicate an agent restart is required.
        _log.debug("Store agent creation time and indicate restart")
        agent_process = wait_for_process(AGENT_EXE, AGENT_PARMS,
                                         MAX_TIME_FOR_AGENT_RESTART)
        install_config["version"] = VERSION
        install_config["agent-created"] = str(agent_process.create_time())
        store_config(NETMODULES_INSTALL_CONFIG, install_config)
        return {RESTART_AGENT}

    # If we updated the agent then check the process start time to determine
    # if we need to restart the agent.
    _log.debug("Restart agent if not using updated config")
    agent_process = wait_for_process(AGENT_EXE, AGENT_PARMS,
                                      MAX_TIME_FOR_AGENT_RESTART)
    if install_config["agent-created"] == str(agent_process.create_time()):
        return {RESTART_AGENT}
    else:
        return None


def cmd_install_docker_cluster_store():
    """
    Install Docker configuration for Docker multi-host networking.
    This command does not block.

    The command writes to stdout the following
      restart-docker-yes
      restart-docker-no
    depending on whether a restart of the docker daemon is required or not.

    This command does the following:
    -  Reads the /etc/calico/installer/docker-start file if it exists.  It
       contains the start time of the docker daemon at the point the
       docker config file was installed or updated.
       -  If the file exists and the time is different to the current time then
          return restart-docker-no.
       -  If the file exists and the time is the same as the current time then
          return restart-docker-yes.
       -  Otherwise:
          -  Update the /etc/docker/daemon.json file to include the cluster
             store information.
          -  Write out the file /etc/calico/installer/docker-start containing a
             timestamp of the docker daemon start time.
          -  Return restart-docker-yes

    :return:
    """
    # Load the current Calico install info for Docker, and the current Docker
    # daemon configuration.
    install_config = load_config(DOCKER_INSTALL_CONFIG)
    daemon_config = load_config(DOCKER_DAEMON_CONFIG)

    if "cluster-store" not in daemon_config:
        # Before updating the config flag that config is updated, but don't yet
        # put in the create time (we only do that after actually updating the
        # config.
        _log.debug("Configure cluster store in daemon config")
        install_config["docker-updated"] = True
        install_config["docker-created"] = None
        store_config(DOCKER_INSTALL_CONFIG, install_config)

        # Update the daemon config.
        daemon_config["cluster-store"] = CLUSTER_STORE_ETCD_PROXY
        store_config(DOCKER_DAEMON_CONFIG, daemon_config)

    if not install_config.get("docker-updated"):
        _log.debug("Docker not updated by Calico")
        return None

    if not install_config.get("docker-created"):
        # Docker configuration was updated, store the current docker process
        # ID and then indicate a Docker restart is required.
        _log.debug("Store docker daemon creation time and indicate restart")
        daemon_process = wait_for_process(DOCKER_EXE, DOCKER_DAEMON_PARMS,
                                          MAX_TIME_FOR_DOCKER_RESTART)
        install_config["version"] = VERSION
        install_config["docker-created"] = str(daemon_process.create_time())
        store_config(DOCKER_INSTALL_CONFIG, install_config)
        return {RESTART_DOCKER}

    # If we updated Docker the check the process start time to determine if we
    # need to restart docker.
    _log.debug("Restart docker if not using updated config")
    daemon_process = wait_for_process(DOCKER_EXE, DOCKER_DAEMON_PARMS,
                                      MAX_TIME_FOR_DOCKER_RESTART)
    if install_config["docker-created"] == str(daemon_process.create_time()):
        return {RESTART_DOCKER}
    else:
        return None


def cmd_restart_components():
    """
    Command to restart the following components:
    -  Docker daemon
    -  Mesos Agent process
    This command does not block.

    If the InstallDockerClusterStore task indicated that a restart is required
    then kill the Docker daemon and wait for it to restart.

    If the InstallNetmodules task indicated that a restart is required then
    kill the agent process.

    :return:
    """
    _log.info("Restarting required components")

    # If a Docker restart is required, kill the process and wait for it to come
    # back online.
    if RESTART_DOCKER in sys.argv:
        _log.debug("Restarting Docker daemon")
        daemon_process = wait_for_process(DOCKER_EXE, DOCKER_DAEMON_PARMS,
                                          MAX_TIME_FOR_DOCKER_RESTART)
        daemon_process.kill()
        _log.debug("Docker daemon killed")

        wait_for_process(DOCKER_EXE, DOCKER_DAEMON_PARMS,
                         MAX_TIME_FOR_DOCKER_RESTART)
        _log.debug("Docker daemon restarted")
    return None


def cmd_get_agent_ip():
    """
    Connects a socket to the DNS entry for mesos master and returns
    which IP address it connected via, which should be the agent's
    accessible IP.
    :return: string representation of agent_ip
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((MESOS_MASTER_HOSTNAME, MESOS_MASTER_PORT))
        agent_ip =  s.getsockname()[0]
        s.close()
    except socket.gaierror:
        # Return an error signal to kill the task, so the process
        # doesn't continue on to launch calicoctl
        _log.error("Unable to connect to master at: %s:%d",
                   MESOS_MASTER_HOSTNAME,
                   MESOS_MASTER_PORT)
        sys.exit(1)

    return agent_ip


if __name__ == "__main__":
    action = sys.argv[1]
    if action == ACTION_INSTALL_NETMODULES:
        components = cmd_install_netmodules()
        output_restart_components(components)
    elif action == ACTION_CONFIGURE_DOCKER:
        components = cmd_install_docker_cluster_store()
        output_restart_components(components)
    elif action == ACTION_RESTART:
        cmd_restart_components()
    elif action == ACTION_GET_AGENT_IP:
        print(cmd_get_agent_ip())
    else:
        print "Unexpected action: %s" % action
        sys.exit(1)
    # TODO: fix logging so that this doesn't get sent to installer's stdout
    # _log.info("Completed")
    sys.exit(0)

