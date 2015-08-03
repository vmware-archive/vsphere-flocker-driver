from flocker.node.agents.blockdevice import (
    VolumeException, AlreadyAttachedVolume,
    UnknownVolume, UnattachedVolume,
    IBlockDeviceAPI, BlockDeviceVolume, _blockdevicevolume_from_dataset_id,
    _blockdevicevolume_from_blockdevice_id
)
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnect, Disconnect

from twisted.python.filepath import FilePath
from zope.interface import implementer
from subprocess import check_output
from bitmath import Byte, GiB, KiB
import base64
import urllib
import urllib2
import json
import os
import re
import socket
import logging
import string
import random
import uuid
import netifaces

class VolumeExists(VolumeException):
    """
    Request for creation of an existing volume
    """


class VolumeAttached(VolumeException):
    """
    Attempting to destroy an attached volume
    """


class InvalidVolumeMetadata(VolumeException):
    """
    Volume queried or supplied has invalid data
    """


class VolumeBackendAPIException(Exception):
    """
    Exception from backed mgmt server
    """


class DeviceException(Exception):
    """
    A base class for exceptions raised by  ``IBlockDeviceAPI`` operations.
    Due to backend device configuration
    """



class DeviceVersionMismatch(DeviceException):
    """
    The version of device not supported.
    """


class DeviceExceptionObjNotFound(Exception):
    """
    The Object not found on device
    """

def get_all_ips():
    """
    Find all IPs for this machine.
    :return: ``set`` of IP address string
    """
    ips = set()
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        for address_family in (netifaces.AF_INET, netifaces.AF_INET6):
            family_addresses = addresses.get(address_family)
            if not family_addresses:
                continue
            for address in family_addresses:
                ips.add(address['addr'])
    return ips

logging.basicConfig(filename='/var/log/flocker/vsphere.log',level=logging.DEBUG)

@implementer(IBlockDeviceAPI)
class VsphereBlockDeviceAPI(object):
    """
    A ``IBlockDeviceAPI`` which creates volumes (vmdks) with vsphere backend.
    """

    def __init__(self, cluster_id, vc_ip, username, password, datacenter_name, datastore_name):
        
        self._cluster_id = cluster_id
        self._vc_ip = vc_ip
        self._username = username
        self._password = password
        self._datacenter_name = datacenter_name
        self._datastore_name = datastore_name
        self.volume_list = {}
        logging.debug("vsphere __init__ : " + str(self._cluster_id) + ": " + self._vc_ip +  ": " + self._username + ": " +  self._password + 
                    ": " + self._datacenter_name + ": " + self._datastore_name)

        self._si = self._connect()
        
        content = self._si.RetrieveContent()
        for childEntity in content.rootFolder.childEntity:
           if childEntity.name == self._datacenter_name:
              self._dc = childEntity
              break

        logging.debug("dc = " + str(self._dc))

        self._flocker_volume_datastore_folder = '[' + self._datastore_name + ']FLOCKER/'
        logging.debug("datastore folder = " + self._flocker_volume_datastore_folder)


    def _connect(self):

        try:
           # Connect to VC
           # si - the root object of inventory
           si = SmartConnect(host=self._vc_ip, port=443, user=self._username, pwd=self._password)
        except vmodl.MethodFault, e:
           logging.error("Caught vmodl fault : " + str(e))
        if not si:
           logging.error("Connection Failed")
           sys.exit()

        return si
    

    def compute_instance_id(self):
        """
        :return: Compute instance id
        """
        content = self._si.RetrieveContent()
        searchIndex = content.searchIndex
        localIps = get_all_ips()
        logging.debug("get_all_ips : " + str(localIps))
        for localIp in localIps:
           vms = searchIndex.FindAllByIp(datacenter=self._dc, ip=localIp, vmSearch=True) 
           logging.debug("vms : " + str(vms))
           if vms:  
              print vms[0]._moId 
              logging.debug("instance id : " + vms[0]._moId) 
              return vms[0]._moId
        
        logging.error("No instance id found")
        raise Exception("No instance id found")

    def allocation_unit(self):
        """
        Return allocation unit
        """
        logging.debug("vsphere allocation_unit: " + str(int(GiB(4).to_Byte().value)))
        return int(GiB(4).to_Byte().value)

    def _id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    def create_volume(self, dataset_id, size):
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager
        logging.debug(virtualDiskManager)

        fileBackedVirtualDiskSpec = vim.VirtualDiskManager.FileBackedVirtualDiskSpec()
        fileBackedVirtualDiskSpec.capacityKb = int(Byte(size).to_KiB().value)
        logging.debug("capacityKb = " + str(fileBackedVirtualDiskSpec.capacityKb))
        fileBackedVirtualDiskSpec.adapterType = 'lsiLogic'
        fileBackedVirtualDiskSpec.diskType = 'thick'
        uuid = ''
        try:
           directory = self._flocker_volume_datastore_folder + str(dataset_id)
           fileManager = content.fileManager
           fileManager.MakeDirectory(name=directory, datacenter=self._dc, createParentDirectories=True)
           path_name= directory  + "/volume-" + self._id_generator() + ".vmdk"
           logging.debug(path_name)
           new_disk = [virtualDiskManager.CreateVirtualDisk_Task(name=path_name, datacenter=self._dc, spec=fileBackedVirtualDiskSpec)]
           self._wait_for_tasks(new_disk, self._si)
           logging.debug("VMDK created successfully")
           uuid = virtualDiskManager.QueryVirtualDiskUuid(name=path_name, datacenter=self._dc)
           logging.debug(str(uuid))
        except vmodl.MethodFault, e:
           logging.error("Caught vmodl fault : " + str(e))

        uuid = uuid.translate(None, ' -')
        logging.debug(uuid)
        volume = BlockDeviceVolume(
                  size=size, 
                  dataset_id=dataset_id, 
                  blockdevice_id=unicode(uuid))
        logging.debug("vsphere create_volume: " + volume.blockdevice_id)

        self.volume_list[str(volume.blockdevice_id)] = volume
        logging.debug("vsphere volume_list: " + str(self.volume_list))

        return volume

    def _wait_for_tasks(self, tasks, si):
        """
        Given the service instance, si, and tasks, it returns after all the
        tasks are complete
        """
        pc = si.content.propertyCollector
        taskList = [str(task) for task in tasks]

        # Create filter
        objSpecs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task) for task in tasks]
        propSpec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task, pathSet=[], all=True)
        filterSpec = vmodl.query.PropertyCollector.FilterSpec()
        filterSpec.objectSet = objSpecs
        filterSpec.propSet = [propSpec]
        filter = pc.CreateFilter(filterSpec, True)

        try:
           version, state = None, None

           # Loop looking for updates till the state moves to a completed state.
           while len(taskList):
              update = pc.WaitForUpdates(version)
              for filterSet in update.filterSet:
                 for objSet in filterSet.objectSet:
                     task = objSet.obj
                     for change in objSet.changeSet:
                         if change.name == 'info':
                            state = change.val.state
                         elif change.name == 'info.state':
                            state = change.val
                         else:
                            continue

                         if not str(task) in taskList:
                            continue

                         if state == vim.TaskInfo.State.success:
                            logging.debug(task.info.result)
                            # Remove task from taskList
                            taskList.remove(str(task))
                         elif state == vim.TaskInfo.State.error:
                            raise task.info.error
           # Move to next version
           version = update.version
        finally:
            if filter:
               filter.Destroy()

    def _find_datastore(self):
        datastores = self._dc.datastore	
        for datastore in datastores:
           if datastore.name == self._datastore_name:
              return datastore
    
    def _find_all_vms(self):
        vmFolder = self._dc.vmFolder
        children = vmFolder.childEntity
        vms = []
        for child in children:
           self._search_all_vms(child, vms)
        print vms
        return vms

    def _search_all_vms(self, child, vms):
        
        if hasattr(child, 'childEntity'):
           for child2 in child.childEntity:
               self._search_all_vms(child2, vms)
        else: 
           vms.append(child)
        
    def destroy_volume(self, blockdevice_id):
        datastore = self._find_datastore()
        print datastore
        datastoreBrowser = datastore.browser
        vmDiskQuery = vim.host.DatastoreBrowser.VmDiskQuery()
        searchSpec = vim.host.DatastoreBrowser.SearchSpec()
        searchSpec.query = [vmDiskQuery] 
 
        searchResultsTask = [datastoreBrowser.SearchDatastore_Task(datastorePath=self._flocker_volume_datastore_folder, searchSpec=searchSpec)]
        self._wait_for_tasks(searchResultsTask, self._si)
        searchResults =  searchResultsTask[0].info.result
        volume_paths = []
        for file in searchResults.file:
            volume_paths.append(searchResults.folderPath + file.path)
        print volume_paths
        
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager
        for volume_path in volume_paths:
            uuid = virtualDiskManager.QueryVirtualDiskUuid(name=volume_path, datacenter=self._dc)
            print uuid
	    uuid = uuid = uuid.translate(None, ' -')
            print uuid
            print unicode(uuid)
            print blockdevice_id
            if unicode(uuid) == blockdevice_id:
               tasks = [virtualDiskManager.DeleteVirtualDisk_Task(name=volume_path, datacenter=self._dc)]
               self._wait_for_tasks(tasks, self._si)
               logging.debug("VMDK deleted successfully")
               break
                
        logging.debug("vsphere destroy_volume: " + str(blockdevice_id))
        


    def attach_volume(self, blockdevice_id, attach_to):
       
        volume = self.volume_list[str(blockdevice_id)]
        attached_volume = volume.set(attached_to=unicode(attach_to))
        self.volume_list[str(blockdevice_id)] = attached_volume
        
        logging.debug("attached_to=" + attached_volume.attached_to)
        logging.debug("vsphere attach_volume: " + str(blockdevice_id) + " : " + attach_to)

        return attached_volume


    def detach_volume(self, blockdevice_id):
        """
        :param: volume id = blockdevice_id
        :raises: unknownvolume exception if not found
        """
        volume = self.volume_list[str(blockdevice_id)]
	if volume.attached_to is not None:
            detached_volume = volume.set(attached_to=None)
            self.volume_list[str(blockdevice_id)] = detached_volume
        else:
            logging.debug("Volume" + blockdevice_id + "not attached")
            raise UnattachedVolume(blockdevice_id)
        
        logging.debug("vsphere detach_volume: " + blockdevice_id)

    def _find_virtual_disks(self):
        datastore = self._find_datastore()
        print datastore

        datastoreBrowser = datastore.browser
        vmDiskQuery = vim.host.DatastoreBrowser.VmDiskQuery()
        
        details = vim.host.DatastoreBrowser.VmDiskQuery.Details()
        details.capacityKb = True
        details.diskType = True
        details.hardwareVersion = True
        filters = vim.host.DatastoreBrowser.VmDiskQuery.Filter()
        diskType =[]
        diskType.append(vim.vm.device.VirtualDisk.FlatVer2BackingInfo)
        filters.diskType = diskType
        vmDiskQuery.details = details
        vmDiskQuery.filter = filters
        searchSpec = vim.host.DatastoreBrowser.SearchSpec()
        fileDetails = vim.host.DatastoreBrowser.FileInfo.Details()
        fileDetails.fileType = True
        fileDetails.fileSize = True
        
        searchSpec.query = [vmDiskQuery]
        searchSpec.details = fileDetails
        print searchSpec
        searchResultsTask = [datastoreBrowser.SearchDatastoreSubFolders_Task(datastorePath=self._flocker_volume_datastore_folder, searchSpec=searchSpec)]
        self._wait_for_tasks(searchResultsTask, self._si)
        searchResults =  searchResultsTask[0].info.result
        print searchResults
        return searchResults       

    def list_volumes(self):
        vms = self._find_all_vms()
        
        searchResults = self._find_virtual_disks()
        volume_paths = []
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager

        block_device_ids = []
        vol_list = {}
        for result in searchResults:
            for file in result.file:
                print file
                volume_path = result.folderPath + file.path
                disk_uuid = virtualDiskManager.QueryVirtualDiskUuid(name=volume_path, datacenter=self._dc)
                print disk_uuid
                disk_uuid = disk_uuid.translate(None, ' -')
                print disk_uuid
                block_device_ids.append(disk_uuid)
                print block_device_ids
                tokens = volume_path.split('/')
                print tokens
                if len(tokens) != 3:
                   # dataset_id is the folder name for the vmdk which is not present so we just skip
                   continue
                str_dataset_id = tokens[1]
                print str_dataset_id
                dataset_id = uuid.UUID(str_dataset_id)
                print dataset_id

                volume = BlockDeviceVolume(
                           size=int(KiB(file.capacityKb).to_Byte().value),
                           dataset_id=dataset_id,
                           blockdevice_id=unicode(disk_uuid))

                for vm in vms:
                    devices = vm.config.hardware.device
                    for device in devices:
                       if hasattr(device.backing, 'diskMode'):
                          diskUuid = device.backing.uuid
                          print diskUuid
                          diskUuid = diskUuid.translate(None, ' -')
                          print diskUuid
                          if diskUuid == disk_uuid:
                              volume.set(attached_to=unicode(vm._moId))
                              
        vol_list[disk_uuid] = volume 
        volumes = []
        for volume in vol_list.values():
            volumes.append(volume)

        print volumes
        logging.debug("vsphere list_volumes: " + str(volumes))
        return volumes

    def get_device_path(self, blockdevice_id):
        """
        :param blockdevice_id:
        :return:the device path
        """
        logging.debug("vsphere get_device_path : " + str(blockdevice_id) + " : " + str(FilePath('/dev/sdc')))

        return FilePath('/dev/sdc')


def vsphere_from_configuration(cluster_id, vc_ip, username, password, datacenter_name, datastore_name):
    
    return VsphereBlockDeviceAPI(
        cluster_id=cluster_id,
        vc_ip=vc_ip,
        username=username,
        password=password,
        datacenter_name=datacenter_name,
        datastore_name=datastore_name
    )

def main():
   vs = vsphere_from_configuration(cluster_id='1',
        vc_ip="10.112.93.71",
        username=u'Administrator@vsphere.local',
        password=u'Admin!23',
        datacenter_name="Datacenter",
        datastore_name="vsanDatastore")
   #vs.create_volume(dataset_id=uuid.uuid4(), size=21474836480)
   #vs.compute_instance_id()
   #vs.destroy_volume(unicode('6000C293807dc7338ce125214e8cb98d'))
   vs.list_volumes()

if __name__ == '__main__':
    main()

