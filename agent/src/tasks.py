
def cmd_run_etcd_proxy():
    """
    Run etcd proxy listening on port 2379 and block.
    :return:
    """


def cmd_install_netmodules():
    """
    Install netmodules and Calico plugin.  This command does not block.

    The command writes to stdout the following
      restart-agent-yes
      restart-agent-no
    depending on whether a restart of the agent is required or not.

    The command does the following:
    -  Reads the /choose/a/directory/agent-start file if it exists.  It
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
          -  Write out the file /choose/a/directory/agent-start containing a
             timestamp of the agent start time.
          -  Return restart-agent-yes
    """


def cmd_install_docker_cluster_store():
    """
    Install Docker configuration for Docker multi-host networking.
    This command does not block.

    The command writes to stdout the following
      restart-docker-yes
      restart-docker-no
    depending on whether a restart of the docker daemon is required or not.

    This command does the following:
    -  Reads the /choose/a/directory/docker-start file if it exists.  It
       contains the start time of the docker daemon at the point the
       docker config file was installed or updated.
       -  If the file exists and the time is different to the current time then
          return restart-docker-no.
       -  If the file exists and the time is the same as the current time then
          return restart-docker-yes.
       -  Otherwise:
          -  Update the /etc/docker/daemon.json file to include the cluster
             store information.
          -  Write out the file /choose/a/directory/docker-start containing a
             timestamp of the docker daemon start time.
          -  Return restart-docker-yes
    """



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
    """



def cmd_run_calico_node():
    """
    Command to run Calico node.  This command blocks while the node is
    running.
    """



def cmd_run_calico_libnetwork:
    """
    Command to run an Calico libnetwork.  This command blocks while the
    libnetwork container is running.
    """


