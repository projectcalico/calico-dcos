<!--- master only -->
> ![warning](docs/images/warning.png) This document applies to the HEAD of the calico-dcos source tree.
>
> View the calico-dcos documentation for the latest release [here](https://github.com/projectcalico/calico-dcos/blob/1.0.0/README.md).
<!--- else
> You are viewing the calico-mesos-deployments documentation for release **release**.
<!--- end of master only -->

# Calico DCOS
This repo contains the implementation of the Calico Universe package for
DCOS.

If you would like to install Calico on your DCOS cluster, please refer 
to the [DCOS Calico Universe installation]() guide.  This guide describes
how to install Calico on your DCOS deployment and provides details for
using Calico in that deployment.

### Nitty gritty

This repo contains a Mesos framework and installation scripts used for
deploying Calico in an existing Mesos cluster.  The DCOS Universe package
for Calico is essentially a wrapper for this framework, providing an
easy to operate user interface.  There is no fundamental reason why this
framework has to be used within the Universe infrastructure, and it 
should be possible to use this on an existing Mesos deployment.

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


[![Analytics](https://calico-ga-beacon.appspot.com/UA-52125893-3/calico-dcos/README.md?pixel)](https://github.com/igrigorik/ga-beacon)
