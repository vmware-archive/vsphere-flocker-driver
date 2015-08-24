VMware vSphere Flocker Driver
======================

## Description
ClusterHQ/Flocker provides an efficient and easy way to connect persistent store with Docker containers. This project provides a plugin to provision persistent data volumes on VMware's vSphere storage backend.

## Installation
- Pre-requisites:
  - Setup vSAN cluster on your VC. (Minimum 3 esx hosts)
  - Let the name of the vsan datastore be say 
   ```bash
   "vsanDatastore"
   ```
  - Create directory with name "FLOCKER" on this datastore. Flocker volumes will reside in this path.
  ```bash
   "[vsanDatastore]/FLOCKER"
   ``` 
  - Deploy ubuntu VMs (which you will use as flocker nodes) on vSAN cluster.
  - Power off these vms and enable disk uuid on these VMs by adding following to the vmx files.
   ```bash
   "disk.EnableUUID​ = TRUE"
   ``` 
  - Power on these vms and install vmware tools
    - Ubuntu<br>
     ```bash
     sudo apt-get install open-vm-tools
     ```
  - Install scsitools
    - Ubuntu<br>
     ```bash
     sudo apt-get install scsitools
     ```
  - Install sg3-utils
    - Ubuntu<br>
     ```bash
     sudo apt-get install sg3-utils
     ```
  - One of these VMs should have flocker control service installed.
    - Follow the ubuntu specific steps on https://docs.clusterhq.com/en/1.0.3/using/installing/index.html​ for installing flocker.
    

- Steps to install vsphere-flocker-driver:
  ```bash
  /opt/flocker/bin/pip install git+https://github.com/vmware/vsphere-flocker-driver.git
  ```
  
## Usage Instructions
To start the plugin on a node, a configuration file must exist on the node at /etc/flocker/agent.yml. This should be as follows:
```bash
version: 1
control-service:
hostname: "{IP/Hostname of Flocker-Control Service}"                
dataset:
backend: "vsphere_flocker_plugin"
vc_ip: "{VC_IP}"                   
username: "{VC_Username}"          
password: "{VC_Password}"          
datacenter_name: "Datacenter"           # VC datacenter name
datastore_name: "vsanDatastore"         # VC VSAN datastore name as above
```

## License

VMware vSphere Flocker Driver is available under the [Apache 2 license](LICENSE).
