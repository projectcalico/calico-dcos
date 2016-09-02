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

"""
Installer binary for installation tasks too complicated for bash.

Usage: installer <command> [<args>...]
    installer docker <cluster-store> Configure docker with cluster store and reboot.
    installer ip <dest-ip:port>      Return the src-IP used to access specified destination.
    installer cni <cni-plugin-dir> <cni-conf-dir> [--public]    Install CNI plugin.
"""

import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from collections import OrderedDict

import psutil

_log = logging.getLogger(__name__)

# Calico installation config files
INSTALLER_CONFIG_DIR = "/etc/calico/installer"
CNI_INSTALL_CONFIG = INSTALLER_CONFIG_DIR + "/cni"
DOCKER_INSTALL_CONFIG = INSTALLER_CONFIG_DIR + "/docker"

# Docker information for a standard Docker install.
DOCKER_DAEMON_CONFIG = "/etc/docker/daemon.json"
DOCKER_SERVICE_NAME = "docker"

# Docker information for a standard Docker install.
AGENT_CONFIG = "/opt/mesosphere/etc/mesos-slave-common"
AGENT_MODULES_CONFIG = "/opt/mesosphere/etc/mesos-slave-modules.json"

# Docker version regex
DOCKER_VERSION_RE = re.compile(r"Docker version (\d+)\.(\d+)\.(\d+).*")

DISTRO_INFO_FILE = "/etc/os-release"

# Max time for process restarts (in seconds)
MAX_TIME_FOR_DOCKER_RESTART = 30
MAX_TIME_FOR_AGENT_RESTART = 30

# Time to wait to check that a process is stable.
PROCESS_STABILITY_TIME = 5


def run_command(command, args=None, paths=None):
    """
    Run a command on the host.
    :return: A tuple of (output, exception).  Either one of output or exception
    will be None.
    """
    args = args or []
    paths = paths or []

    # Locate the command from the possible list of paths (if supplied).
    for path in paths:
        new_command = os.path.join(path, command)
        if os.path.exists(new_command):
            command = new_command
    _log.debug("Executing command: %s", command)

    try:
        res = subprocess.check_output([command] + args)
    except subprocess.CalledProcessError, e:
        _log.exception("CalledProcessError running command: %s", e)
        return None, e
    except OSError, e:
        _log.exception("OSError running command: %s", e)
        return None, e
    else:
        _log.debug("Command output: %s", res)
        return res.strip(), None


def docker_version_supported():
    """
    Check if the host has a version of Docker that is supported.
    :return: True if supported.
    """
    res, exc = run_command("docker", args=["--version"],
                           paths=["/usr/bin", "/bin"])

    if exc:
        _log.warning("Unable to query Docker version")
        return False

    version_match = DOCKER_VERSION_RE.match(res)
    if not version_match:
        _log.warning("Docker version output unexpected format")
        return False

    vmajor = int(version_match.group(1))
    vminor = int(version_match.group(2))
    vpatch = int(version_match.group(3))
    if (vmajor, vminor, vpatch) < (1, 10, 0):
        _log.warning("Docker version is not supported")
        return False

    _log.debug("Docker version is supported")
    return True


def restart_service(service_name):
    """
    Restart the process using systemctl if possible (otherwise kill the
    process)
    :param service_name:
    :param process:
    """
    res, exc = run_command("systemctl", args=["restart", service_name],
                           paths=["/usr/bin", "/bin"])

    if exc and isinstance(exc, OSError):
        _log.error("Error restarting with systemctl")


def start_service(service_name):
    """
    Start the process using systemctl if possible.
    :param service_name:
    """
    run_command("systemctl", args=["start", service_name],
                paths=["/usr/bin", "/bin"])


def ensure_dir(directory):
    """
    Ensure the specified directory exists
    :param directory:
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


class ProcessNotFound(Exception):
    pass


def wait_for_service(service_name, max_wait=1):
    """
    Locate the specified process, waiting a specified max time before giving
    up.  If the process can not be found within the time limit, the script
    exits.
    :param max_wait:  The maximum time to wait for the process to appear.
    :param stability_time:  The time to wait to check for process stability.
    :return:
    """
    start = time.time()
    exc = 1
    while exc and time.time() <= start + max_wait:
        time.sleep(1)
        _, exc = run_command("systemctl",
                          args=["is-active", service_name],
                          paths=["/usr/bin", "/bin"])

    if exc:
        _log.warning("Service not up within timeout: %s" % service_name)
        raise ProcessNotFound()


def load_config(filename):
    """
    Load a JSON config file from disk.
    :param filename:  The filename of the config file
    :return:  A dictionary containing the JSON data.  If the file was not found
    an empty dictionary is returned.
    """
    if os.path.exists(filename):
        _log.debug("Reading config file: %s", filename)
        with open(filename) as f:
            config = json.loads(f.read())
    else:
        _log.debug("Config file does not exist: %s", filename)
        config = {}
    _log.debug("Returning config:\n%s", config)
    return config


def store_config(filename, config):
    """
    Store the supplied config as a JSON file.  This performs an atomic write
    to the config file by writing to a temporary file and then renaming the
    file.
    :param filename:  The filename
    :param config:  The config (a simple dictionary)
    """
    ensure_dir(os.path.dirname(filename))
    atomic_write(filename, json.dumps(config))


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
            props[line[0].strip()] = line[1].split(",")
    _log.debug("Read property file:\n%s", props)
    return props


def store_property_file(filename, props):
    """
    Write a property file (see load_property_file)
    :param filename:
    :param props:
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


def move_file_if_missing(from_file, to_file):
    """
    Checks if the destination file exists and if not moves it into place.
    :param from_file:
    :param to_file:
    :return: Whether the file was moved.
    """
    _log.debug("Move file from %s to %s", from_file, to_file)
    if not os.path.exists(from_file):
        _log.error("From file does not exist.")
        return False
    if os.path.exists(to_file):
        _log.debug("File %s already exists, not copying", to_file)
        return False
    tmp_to_file = to_file + ".tmp"

    # We cannot use os.rename() because that does not work across devices.  To
    # ensure we still do an atomic move, copy the file to a temporary location
    # and then rename it.
    ensure_dir(os.path.dirname(to_file))
    shutil.move(from_file, tmp_to_file)
    os.rename(tmp_to_file, to_file)
    return True


def get_host_info():
    """
    Gather information from the host OS.
    :return: A tuple containing the mesos-version, distribution name, architecture,
      in that order. Contents of any of the three tuple values may be None if
      we were unable to detect them.
    """
    _log.info("Gathering host information.")

    # Get Architecture
    arch, _ = run_command("uname", args=["-m"], paths=["/usr/bin", "/bin"])
    _log.info("Arch: %s", arch)

    # Get Mesos Version. (We use mesos-master since mesos-slave --version
    # fails unless --work-dir is also passed in: 
    # https://issues.apache.org/jira/browse/MESOS-5928)
    raw_mesos_version, _ = run_command("mesos-master", args=["--version"],
                                       paths=["/opt/mesosphere/bin"])
    mesos_version = raw_mesos_version.split(" ")[1] if raw_mesos_version else None
    _log.info("Mesos Version: %s" % mesos_version)

    # Get Distribution
    distro = None
    release_info = load_property_file(DISTRO_INFO_FILE)
    if release_info:
        if 'ID' in release_info:
            # Remove quotes surrounding distro
            distro = release_info['ID'][0].replace('\"', '')
        else:
            _log.error("Couldn't find ID field in %s", DISTRO_INFO_FILE)
    else:
        _log.error("Couldn't find release-info file: %s", DISTRO_INFO_FILE)

    _log.info("Distro: %s", distro)
    return mesos_version, distro, arch


def cmd_install_cni(public_slave, cni_plugins_dir, cni_conf_dir, etcd_endpoints):
    """
    Install Calico's CNI plugin.  A successful completion of the task
    indicates successful installation

    :param public_slave: Flag indicating if this is a public slave.
    """
    # Load the current Calico install info for CNI
    install_config = load_config(CNI_INSTALL_CONFIG)

    if public_slave:
        agent_service_name = "dcos-mesos-slave-public"
    else:
        agent_service_name = "dcos-mesos-slave"

    # Before starting the install, check that we are able to locate the agent
    # process - if not there is not much we can do here.
    if not install_config:
        _log.debug("Have not started installation yet")
        # noinspection PyTypeChecker
        try:
            wait_for_service(agent_service_name, MAX_TIME_FOR_AGENT_RESTART)
        except ProcessNotFoundException:
            _log.error("Cannot find agent process - refusing to continue.")
            sys.exit(1)

        mesos_version, _, _ = get_host_info()
        # CNI supported on Mesos 1.0.0+, check major release is at least 1.0.
        if int(mesos_version.split(".")[0]) < 1:
            _log.error("Mesos version does not support CNI. Performing a no-op.")
            return

        install_config["restarted-agent"] = None
        install_config["cni-installed"] = None
        store_config(CNI_INSTALL_CONFIG, install_config)

    if not install_config.get("cni-bin-installed"):
        cni_conf = {
            "name": "calico",
            "type": "calico",
            "etcd_endpoints": etcd_endpoints,
            "ipam": {
                "type": "calico-ipam"
            }
        }
        store_config("%s/calico.cni" % cni_conf_dir, cni_conf)
        move_file_if_missing("./calico", "%s/calico" % cni_plugins_dir)
        move_file_if_missing("./calico-ipam", "%s/calico-ipam" % cni_plugins_dir)

        install_config["cni-installed"] = True
        store_config(CNI_INSTALL_CONFIG, install_config)

    # If we haven't stored the current agent creation time, then do so now.
    # We use this to track when the agent has restarted with our new config.
    if not install_config.get("restarted-agent"):
        install_config["restarted-agent"] = True
        store_config(CNI_INSTALL_CONFIG, install_config)

        restart_service(agent_service_name)
        sys.exit(1)

    wait_for_service(agent_service_name, MAX_TIME_FOR_AGENT_RESTART)
    time.sleep(PROCESS_STABILITY_TIME)
    wait_for_service(agent_service_name)

    _log.debug("Agent was restarted and is stable since config was updated")
    return


def cmd_install_docker_cluster_store(cluster_store):
    """
    Install Docker configuration for Docker multi-host networking.  A successful
    completion of the task indicates successful installation.
    """
    # Check if the Docker version is supported.  If not, just finish the task.
    if not docker_version_supported():
        _log.info("Docker version is not supported - finish task")
        return

    # Load the current Calico install info for Docker, and the current Docker
    # daemon configuration.
    install_config = load_config(DOCKER_INSTALL_CONFIG)
    daemon_config = load_config(DOCKER_DAEMON_CONFIG)

    # Before starting the install, check that we can locate the Docker process.
    if not install_config:
        _log.debug("Have not started installation yet")
        try:
            wait_for_service(DOCKER_SERVICE_NAME, MAX_TIME_FOR_DOCKER_RESTART)
        except ProcessNotFound:
            _log.error("Docker is not running on this host. Refusing to continue with update.")
            sys.exit(1)
        # If docker already using a cluster store, we're done here.
        if "cluster-store" in daemon_config:
            _log.info("Docker already using cluster store. Not updating.")
            return

    # Configure docker with cluster store
    if not install_config.get("configured-docker"):
        _log.debug("Configure cluster store in daemon config")
        install_config["configured-docker"] = True
        store_config(DOCKER_INSTALL_CONFIG, install_config)

        # Set cluster-store in the daemon config.
        daemon_config["cluster-store"] = cluster_store
        store_config(DOCKER_DAEMON_CONFIG, daemon_config)

    # Restart Docker
    if not install_config.get("docker-attempted-restart"):
        install_config["docker-attempt-restart"] = True
        store_config(DOCKER_INSTALL_CONFIG, install_config)

        _log.info("Restarting Docker...")
        restart_service(DOCKER_SERVICE_NAME)

    # Wait for docker to come back up
    _log.info("Waiting for Docker to come back online...")
    try:
        wait_for_service(DOCKER_SERVICE_NAME, MAX_TIME_FOR_DOCKER_RESTART)
    except ProcessNotFound:
        _log.error("Expected Docker to come back up, but it did not.")
        sys.exit(1)

    _log.info("Docker's back online. Waiting a bit to check that it's stable.")
    time.sleep(PROCESS_STABILITY_TIME)
    try:
        wait_for_service(DOCKER_SERVICE_NAME)
    except ProcessNotFound as e:
        _log.error("Docker is flapping.")
        raise e

    _log.info("Docker was restarted and is stable since adding cluster store")


def cmd_get_agent_ip():
    """
    Connects a socket to the DNS entry for mesos master and returns
    which IP address it connected via, which should be the agent's
    accessible IP.

    Prints the IP to stdout
    """
    # A comma separated list of host:port is supplied as the only argument.
    for host, port in (hp.split(":") for hp in sys.argv[2].split(",")):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((host, int(port)))
            our_ip = s.getsockname()[0]
            s.close()
            print our_ip
            return
        except socket.gaierror:
            continue
    _log.error("Failed to connect to any of: %s", sys.argv[2])
    sys.exit(1)


def initialise_logging():
    """
    Initialise logging to stdout.
    """
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s]\t%(name)s %(lineno)d: %(message)s')
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":
    initialise_logging()
    _log.info("args: %s", sys.argv)
    action = sys.argv[1]
    if action == "docker":
        cmd_install_docker_cluster_store(cluster_store=sys.argv[2])
    elif action == "ip":
        cmd_get_agent_ip()
    elif action == "cni":
        public_slave = "--public" in sys.argv
        cmd_install_cni(public_slave=public_slave,
                        cni_plugins_dir=sys.argv[2],
                        cni_conf_dir=sys.argv[3],
                        etcd_endpoints=sys.argv[4])
    else:
        print "Unexpected action: %s" % action
        sys.exit(1)
    sys.exit(0)
