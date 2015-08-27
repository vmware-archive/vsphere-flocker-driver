# [VMware vSphere®](http://www.vmware.com/products/vsphere) [Flocker](https://clusterhq.com/flocker/introduction/) Driver

[ClusterHQ](https://clusterhq.com)'s [Flocker](https://clusterhq.com/flocker/introduction/) provides an efficient and easy way to connect persistent store with [Docker](http://docker.com) containers. This project provides a plugin to provision persistent data volumes on VMware's vSphere storage backend.

## Installation

### Pre-requisites:
  - Setup a shared datastore on your VC.
  - Let the name of the datastore be say: `vsphereDatastore`
  - Create directory with name "FLOCKER" on this datastore. Flocker volumes will reside in this path.
    `[vsphereDatastore]FLOCKER`

### Install:

- Deploy Ubuntu VMs (which you will use as Flocker nodes) on ESXi hosts with `vsphereDatastore` as the shared datastore.
  - Power off these VMs and enable disk uuid on these VMs by adding following to the vmx files.
   ```bash
   disk.EnableUUID​ = "TRUE"
   ```
  - Power on these VMs and install VMware Tools, scsitools and sg3-utils
    - Ubuntu<br>
     ```bash
     sudo apt-get install -y open-vm-tools scsitools sg3-utils
     ```
  - One of these VMs should have Flocker control service installed.
    - Follow the Ubuntu specific steps on [this guide](https://docs.clusterhq.com/en/1.0.3/using/installing/index.html​) for installing Flocker.


- Steps to install `vsphere-flocker-driver`:
  ```bash
  /opt/flocker/bin/pip install git+https://github.com/vmware/vsphere-flocker-driver.git
  ```

## Usage Instructions
To start the plugin on a node, a configuration file must exist on the node at `/etc/flocker/agent.yml`. This should be as follows:
```yaml
version: 1
control-service:
  hostname: "{IP/Hostname of Flocker-Control Service}"
dataset:
  backend: "vsphere_flocker_plugin"
  vc_ip: "{VC_IP}"
  username: "{VC_Username}"
  password: "{VC_Password}"
  datacenter_name: "Datacenter"           # VC datacenter name
  datastore_name: "vsphereDatastore"         # VC datastore name as above
```

Please see configuration examples in the [config directory](vsphere_flocker_plugin/config/).

## Legal

Copyright © 2015 VMware, Inc.  All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the “License”); you may not
use this file except in compliance with the License.  You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an “AS IS” BASIS, without warranties or
conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
specific language governing permissions and limitations under the License.
