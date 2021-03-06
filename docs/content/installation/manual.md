---
title: Manual Installation
layout: docs
---

# Manual Installation

Flynn can be installed using our install script on Ubuntu 14.04 amd64.

We recommend starting with a clean Ubuntu installation on machines with at least
1GB of RAM, 20GB of storage, and two CPU cores each.

Before we get going with the installation, please note that if you plan on running a multi-node
cluster, you should boot at least 3 nodes for cluster stability.

*NOTE: If you are installing on Linode, you need to use native kernels (rather
than Linode kernels) for AUFS support, see [this
guide](https://www.linode.com/docs/tools-reference/custom-kernels-distros/run-a-distributionsupplied-kernel-with-pvgrub)
for instructions on how to switch.*

## Installation

Download and run the Flynn install script:

```
$ sudo bash < <(curl -fsSL https://dl.flynn.io/install-flynn)
```

If you would rather take a look at the contents of the script before running it as root, download and
run it in two separate steps:

```
$ curl -fsSL -o /tmp/install-flynn https://dl.flynn.io/install-flynn
... take a look at the contents of /tmp/install-flynn ...
$ sudo bash /tmp/install-flynn
```

Running the installer script will:

1. Install Flynn's runtime dependencies
2. Download, verify and install the `flynn-host` binary
3. Download and verify filesystem images for each of the Flynn components
4. Install an Upstart job for controlling the `flynn-host` daemon

Some of the filesystem images are quite large (hundreds of MB) so step 3 could take a while depending on
your internet connection.

## Rinse and repeat

You should install Flynn as above on every host that you want to be in the Flynn cluster.

## Start Flynn

First, ensure that all network traffic is allowed between all nodes in the cluster (specifically
all UDP and TCP packets). The following ports should also be open externally on the firewalls
for all nodes in the cluster:

* 80 (HTTP)
* 443 (HTTPS)
* 3000 to 3500 (user-defined TCP services, optional)

**Note:** A firewall with this configuration is _required_ to prevent external
access to internal management APIs.

The next step is to configure a Layer 0 cluster by starting the flynn-host
daemon on all nodes. The daemon uses Raft for leader election, and it needs to
be aware of all of the other nodes for it to function correctly.

If you are starting more than one node, the cluster should be configured using
a discovery token.  `flynn-host init` is a tool that handles generating and
configuring the token.

On the first node, create a new token with the `--init-discovery` flag. The
minimum multi-node cluster size is three, and this command does not need to be
run if you are only starting a single node.

```
$ sudo flynn-host init --init-discovery
https://discovery.flynn.io/clusters/53e8402e-030f-4861-95ba-d5b5a91b5902
```

On the rest of the nodes, configure the generated discovery token:

```
$ sudo flynn-host init --discovery https://discovery.flynn.io/clusters/53e8402e-030f-4861-95ba-d5b5a91b5902
```

Then, start the daemon by running:

```
$ sudo start flynn-host
```

You can check the status of the daemon by running:

```
$ sudo status flynn-host
flynn-host start/running, process 4090
```

If the status is `stop/waiting`, the daemon has failed to start for some reason. Check the
log file (`/var/log/upstart/flynn-host.log`) for any errors and try starting the daemon
again.

## Bootstrap Flynn

After you have running `flynn-host` instances, you can now bootstrap the cluster
with `flynn-host bootstrap`. You'll need a domain name with DNS A records
pointing to every node IP address and a second, wildcard domain CNAME to the
cluster domain.

**Example**

```
demo.localflynn.com.    A      192.168.84.42
demo.localflynn.com.    A      192.168.84.43
demo.localflynn.com.    A      192.168.84.44
*.demo.localflynn.com.  CNAME  demo.localflynn.com.
```

*If you are just using a single node and don't want to initially setup DNS
records, you can use [xip.io](http://xip.io) which provides wildcard DNS for
any IP address.*

Set `CLUSTER_DOMAIN` to the main domain name and start the bootstrap process,
specifying the number of hosts that are expected to be present and the discovery
token if you created one.

```
$ sudo \
    CLUSTER_DOMAIN=demo.localflynn.com \
    flynn-host bootstrap \
    --min-hosts 3 \
    --discovery https://discovery.flynn.io/clusters/53e8402e-030f-4861-95ba-d5b5a91b5902
```

*Note: You only need to run this on a single node in the cluster. It will
schedule jobs on nodes across the cluster as required.*

The Layer 1 bootstrapper will get all necessary services running using the Layer
0 API. The final log line will contain configuration that may be used with the
[command-line interface](/docs/cli).

If you try these instructions and run into issues, please open an issue or pull
request.

Now you have Flynn installed and running, head over to the [Using Flynn](/docs)
page for guides on deploying your applications to Flynn.
