<!--- master only -->
> ![warning](docs/images/warning.png) This document applies to the HEAD of the calico-dcos source tree.
>
> View the calico-dcos documentation for the latest release [here](https://github.com/projectcalico/calico-dcos/blob/1.0.0/README.md).
<!--- else
> You are viewing the calico-mesos-deployments documentation for release **release**.
<!--- end of master only -->

# Calico DCOS
This repo contains the source code implementation of the Calico Universe package for
DCOS.

If you would simply like to install Calico on your DCOS cluster, please refer 
to the [DCOS Calico Universe installation](https://github.com/projectcalico/calico-containers/blob/master/docs/mesos/DCOS.md) guide, which describes
how to install Calico on your DCOS cluster and provides details for
using Calico.

### Nitty gritty

The DCOS Universe Calico package exposes an easy to operate Browser
user interface and Calico install, within the DCOS Universe Web GUI.
For more information - https://github.com/mesosphere/universe

This repo contains Universe scripts used for deploying Calico in an existing
DCOS cluster.   The DCOS Universe Calico package is a wrapper for a Mesos framework
and installation scripts used for an existing Mesos cluster.  So, alternatively, it 
should be possible to use this framework code on an existing Mesos deployment.

The Calico framework performs the following operations:
-  Runs an etcd proxy on each agent, so etcd is accessible from port
   2379 on localhost.
-  Updates the Docker daemon configuration on each agent to use etcd as
   its cluster store.  If Docker is already configured to use a cluster
   store, then the framework will not modify the configuration.  The
   framework will restart Docker on each agent, if required, to pick up
   the new config.  Restarts are rate limited with a configurable option
   to say how many agents may be updated at the same time.
-  Updates the Agent configuration to use the Mesos net-modules 
   functionality and Calico Mesos netmodules plugin.  The
   framework will restart each agent, if required, to pick up
   the new config.  Restarts are rate limited with a configurable option
   to say how many agents may be updated at the same time.
-  Runs calico-node and calico-libnetwork container images on each node.
 
The framework also runs a webserver that is used for displaying 
service status.


[![Analytics](https://calico-ga-beacon.appspot.com/UA-52125893-3/calico-dcos/README.md?pixel)](https://github.com/igrigorik/ga-beacon)
