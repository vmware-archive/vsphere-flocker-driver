'''
Copyright 2015 VMware, Inc.  All rights reserved.  Licensed under the Apache v2 License.
'''
from flocker.node.agents.blockdevice import (
    VolumeException, AlreadyAttachedVolume,
    UnknownVolume, UnattachedVolume,
    IBlockDeviceAPI, BlockDeviceVolume
)
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnect

from twisted.python.filepath import FilePath
from zope.interface import implementer
from subprocess import check_output
from bitmath import Byte, GiB, KiB
import logging
import ssl
import uuid
import netifaces
import time

logging.basicConfig(filename='/var/log/flocker/vsphere.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(message)s')


class VolumeAttached(VolumeException):
    """
    Attempting to destroy an attached volume
    """


class VcConnection(Exception):
    """
    Connection to VC failed
    """


class VmNotFound(Exception):
    """
    Local vm instance not found in VC
    """


class VolumeCreationFailure(Exception):
    """
    Volume creation failed
    """


class UnknownVm(Exception):
    """
    VM not found
    """


class VolumeDestroyFailure(Exception):
    """
    destroy volume failed
    """


class VolumeAttachFailure(Exception):
    """
    attach volume failed
    """


class VolumeDetachFailure(Exception):
    """
    detach volume failed
    """


class ListVolumesFailure(Exception):
    """
    list volumes failed
    """


class GetDevicePathFailure(Exception):
    """
    get_device_path failed
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


class VsphereBlockDeviceVolume:
    """
    Data object representing vsphere's BlockDeviceVolume
    """

    def __init__(self, blockDeviceVolume, path):
        self.blockDeviceVolume = blockDeviceVolume
        self.path = path
        self.vm = None
        self.device = None

    def __str__(self):
        return "VsphereBlockDeviceVolume: {" + str(self.blockDeviceVolume) + ", path: " + self.path + ", vm: " + str(self.vm) + ", device: " + str(self.device) + "}"


@implementer(IBlockDeviceAPI)
class VsphereBlockDeviceAPI(object):

    """
    A ``IBlockDeviceAPI`` which creates volumes (vmdks) with vsphere backend.
    """

    def __init__(self, cluster_id, vc_ip, username, password, datacenter_name, datastore_name, validate_cert):

        self._cluster_id = cluster_id
        self._vc_ip = vc_ip
        self._username = username
        self._password = password
        self._datacenter_name = datacenter_name
        self._datastore_name = datastore_name
        self.validate_cert = validate_cert
        logging.debug("vsphere __init__ : {}; {}; {}; {}; {}".format(self._cluster_id, self._vc_ip,
                                                                     self._username, self._datacenter_name,
                                                                     self._datastore_name))

        self._connect()

        content = self._si.RetrieveContent()
        for childEntity in content.rootFolder.childEntity:
            if childEntity.name == self._datacenter_name:
                self._dc = childEntity
                break

        logging.debug("dc = " + str(self._dc))

        self._flocker_volume_datastore_folder = '[' + \
            self._datastore_name + ']FLOCKER/'
        logging.debug("datastore folder = " +
                      self._flocker_volume_datastore_folder)

    def _connect(self):

        try:
            # Connect to VC
            # si - the root object of inventory
            if self.validate_cert:
                self._si = SmartConnect(host=self._vc_ip, port=443,
                                        user=self._username, pwd=self._password,
                                        sslContext=ssl._create_unverified_context())
            else:
                self._si = SmartConnect(host=self._vc_ip, port=443,
                                        user=self._username, pwd=self._password)
        except vmodl.MethodFault as e:
            logging.error("Connection to VC failed with error : " + str(e))
            raise VcConnection(e)

    def compute_instance_id(self):
        """
        Get an identifier for this node.
        This will be compared against ``BlockDeviceVolume.attached_to``
        to determine which volumes are locally attached and it will be used
        with ``attach_volume`` to locally attach volumes.
        :returns: A ``unicode`` object giving a provider-specific node
            identifier which identifies the node where the method is run.

        For vsphere local VM's moref is returned as instance_id
        """
        try:
            content = self._si.RetrieveContent()
            searchIndex = content.searchIndex
            localIps = get_all_ips()
            logging.debug("get_all_ips : " + str(localIps))
            for localIp in localIps:
                vms = searchIndex.FindAllByIp(
                    datacenter=self._dc, ip=localIp, vmSearch=True)
                logging.debug("vms : " + str(vms))
                if vms:
                    logging.debug("instance id : " + vms[0]._moId)
                    return vms[0]._moId
        except Exception as e:
            logging.error("Could not find vm because of error : " + str(e))
            raise VmNotFound(e)

        logging.error("This vm instance is not found in VC")
        raise VmNotFound("This vm instance is not found in VC")

    def allocation_unit(self):
        """
        Return allocation unit
        """
        logging.debug("vsphere allocation_unit: " +
                      str(int(GiB(4).to_Byte().value)))
        return int(GiB(4).to_Byte().value)

    @staticmethod
    def _normalize_uuid(uuid):
        """
        Normalizes the input uuid to lower-case string
        without any white space or '-'
        """
        if uuid:
            uuid = uuid.translate(None, " -\n'").lower()
        return uuid

    def create_volume(self, dataset_id, size):
        """
        Create a new volume.
        Creates a new vmdk volume on vSphere datastore provided in the configuration
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :raises VolumeCreationFailure: If the volume creation fails with some error.
        :returns: A ``BlockDeviceVolume``.
        """

        try:
            content = self._si.RetrieveContent()
            virtualDiskManager = content.virtualDiskManager

            fileBackedVirtualDiskSpec = vim.VirtualDiskManager.FileBackedVirtualDiskSpec()
            fileBackedVirtualDiskSpec.capacityKb = int(
                Byte(size).to_KiB().value)
            logging.debug("capacityKb = " +
                          str(fileBackedVirtualDiskSpec.capacityKb))
            fileBackedVirtualDiskSpec.adapterType = 'lsiLogic'
            fileBackedVirtualDiskSpec.diskType = 'thick'

            path_name = self._flocker_volume_datastore_folder + \
                str(dataset_id) + ".vmdk"
            logging.debug(path_name)
            new_disk = [virtualDiskManager.CreateVirtualDisk_Task(
                name=path_name, datacenter=self._dc, spec=fileBackedVirtualDiskSpec)]
            self._wait_for_tasks(new_disk, self._si)
            logging.debug("VMDK created successfully")
            uuid = virtualDiskManager.QueryVirtualDiskUuid(
                name=path_name, datacenter=self._dc)
            logging.debug(str(uuid))
        except Exception as e:
            logging.error(
                "Cannot create volume because of exception : " + str(e))
            raise VolumeCreationFailure(e)

        uuid = self._normalize_uuid(uuid)
        logging.debug(uuid)
        volume = BlockDeviceVolume(
            size=size,
            dataset_id=dataset_id,
            blockdevice_id=unicode(uuid))
        logging.debug("vsphere create_volume: " + volume.blockdevice_id)
        return volume

    def _wait_for_tasks(self, tasks, si):
        """
        Given the service instance, si, and tasks, it returns after all the
        tasks are complete
        """
        pc = si.content.propertyCollector
        taskList = [str(task) for task in tasks]

        # Create filter
        objSpecs = [vmodl.query.PropertyCollector.ObjectSpec(
            obj=task) for task in tasks]
        propSpec = vmodl.query.PropertyCollector.PropertySpec(
            type=vim.Task, pathSet=[], all=True)
        filterSpec = vmodl.query.PropertyCollector.FilterSpec()
        filterSpec.objectSet = objSpecs
        filterSpec.propSet = [propSpec]
        filter = pc.CreateFilter(filterSpec, True)

        try:
            version, state = None, None

            # Loop looking for updates till the state moves to a completed
            # state.
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

    def _find_vm(self, moid):
        for vm in self._find_all_vms():
            if unicode(vm._moId) == moid:
                return vm

    def _find_all_vms(self):
        vmFolder = self._dc.vmFolder
        children = vmFolder.childEntity
        vms = []
        for child in children:
            self._search_all_vms(child, vms)
        logging.debug(str(vms))
        return vms

    def _search_all_vms(self, child, vms):

        if hasattr(child, 'childEntity'):
            for childEntity in child.childEntity:
                self._search_all_vms(childEntity, vms)
        else:
            vms.append(child)

    def _get_vsphere_blockdevice_volume(self, blockdevice_id):

        vol_list = self._list_vsphere_volumes()
        if not blockdevice_id in vol_list.keys():
            logging.error(
                "Volume not found for blockdevice_id : " + blockdevice_id)
            raise UnknownVolume(blockdevice_id)

        return vol_list[blockdevice_id]

    def _delete_vmdk(self, vsphere_volume):
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager
        tasks = [virtualDiskManager.DeleteVirtualDisk_Task(
            name=vsphere_volume.path, datacenter=self._dc)]
        self._wait_for_tasks(tasks, self._si)
        logging.debug("VMDK deleted successfully")
        logging.debug("vsphere destroy_volume: " + str(blockdevice_id))

    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.
        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises VolumeAttached: If the volume is attached to any vm.
        :raise VolumeDestroFailure: If destroy volume fails with some error.
        :return: ``None``
        """
        vsphere_volume = self._get_vsphere_blockdevice_volume(blockdevice_id)
        if vsphere_volume.blockDeviceVolume.attached_to is not None:
            logging.error("Volume is attached to a vm so cannot destroy.")
            raise VolumeAttached(blockdevice_id)

        try:
            self._delete_vmdk(vsphere_volume)
        except Exception as e:
            logging.error("Destroy volume failed due to error " + str(e))
            raise VolumeDestroyFailure(e)

    def _attach_vmdk(self, vm, vsphere_volume):
        spec = vim.vm.ConfigSpec()
        # get all disks on a VM, set unit_number to the next available
        for dev in vm.config.hardware.device:
            if hasattr(dev.backing, 'fileName'):
                unit_number = int(dev.unitNumber) + 1
                # unit_number 7 reserved for scsi controller
                if unit_number == 7:
                    unit_number += 1
                if unit_number >= 16:
                    logging.debug("we don't support more than 15 disks")
                    raise Exception("VM doesn't support more than 15 disks")

            if isinstance(dev, vim.vm.device.VirtualSCSIController):
                controller = dev

        dev_changes = []
        new_disk_kb = int(
            Byte(vsphere_volume.blockDeviceVolume.size).to_KiB().value)
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add

        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        disk_spec.device.backing.thinProvisioned = False
        disk_spec.device.backing.diskMode = 'persistent'
        disk_spec.device.backing.fileName = vsphere_volume.path
        disk_spec.device.unitNumber = unit_number

        disk_spec.device.capacityInKB = new_disk_kb
        disk_spec.device.controllerKey = controller.key
        dev_changes.append(disk_spec)
        spec.deviceChange = dev_changes
        tasks = [vm.ReconfigVM_Task(spec=spec)]
        self._wait_for_tasks(tasks, self._si)

        volume = vsphere_volume.blockDeviceVolume
        attached_volume = volume.set('attached_to', unicode(vm._moId))
        return attached_volume

    def attach_volume(self, blockdevice_id, attach_to):
        """
        Attach ``blockdevice_id`` to the node indicated by ``attach_to``.
        :param unicode blockdevice_id: The unique identifier for the block
            device being attached.
        :param unicode attach_to: An identifier like the one returned by the
            ``compute_instance_id`` method indicating the node to which to
            attach the volume.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises AlreadyAttachedVolume: If the supplied ``blockdevice_id`` is
            already attached.
        :raises UnknownVm: If the supplied ``attach_to`` vm does not
            exist.
        :raises VolumeAttachFailure: If the attach volume failed with some error.
            exist.
        :returns: A ``BlockDeviceVolume`` with a ``attached_to`` attribute set
            to ``attach_to``.
        """
        vm = self._find_vm(attach_to)
        if vm is None:
            raise UnknownVm("VM not found " + str(attach_to))

        logging.debug(str(vm))

        vsphere_volume = self._get_vsphere_blockdevice_volume(blockdevice_id)
        if vsphere_volume.blockDeviceVolume.attached_to is not None:
            logging.error("Volume is attached to a vm so cannot attach.")
            raise AlreadyAttachedVolume(blockdevice_id)

        try:
            attached_volume = self._attach_vmdk(vm, vsphere_volume)
        except Exception as e:
            logging.error(
                "Cannot attach volume because of exception : " + str(e))
            raise VolumeAttachFailure(e)

        logging.debug("attached_to=" + attached_volume.attached_to)
        logging.debug("vsphere attach_volume: " +
                      str(blockdevice_id) + " : " + attach_to)
        logging.debug(str(attached_volume))

        logging.debug("Rescanning scsi bus for attached disk")
        self._rescan_scsi()
        return attached_volume

    def _rescan_scsi(self):
        try:
            output = check_output(["rescan-scsi-bus", "-r"])
            logging.debug(output)
        except Exception as e:
            ''' Don't throw error during rescan-scsi-bus '''
            logging.warn("Rescan scsi bus failed.")

    def _detach_vmdk(self, vsphere_volume):
        vm = vsphere_volume.vm
        spec = vim.vm.ConfigSpec()
        dev_changes = []

        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.remove
        disk_spec.device = vsphere_volume.device
        dev_changes.append(disk_spec)
        spec.deviceChange = dev_changes

        tasks = [vm.ReconfigVM_Task(spec=spec)]
        self._wait_for_tasks(tasks, self._si)

        volume = vsphere_volume.blockDeviceVolume
        detached_volume = volume.set('attached_to', None)
        return detached_volume

    def detach_volume(self, blockdevice_id):
        """
        Detach ``blockdevice_id`` from whatever host it is attached to.
        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :raises VolumeDetachFailure: If volume detach fails due to some error
        :returns: ``None``
        """
        vsphere_volume = self._get_vsphere_blockdevice_volume(blockdevice_id)
        if vsphere_volume.blockDeviceVolume.attached_to is None:
            logging.debug("Volume " + blockdevice_id + " not attached")
            raise UnattachedVolume(blockdevice_id)

        try:
            detached_volume = self._detach_vmdk(vsphere_volume)
        except Exception as e:
            logging.error("Detach volume failed with error: " + str(e))
            raise VolumeDetachFailure(e)

        self._rescan_scsi()
        logging.debug("vsphere detach_volume: " + blockdevice_id)

    def _find_virtual_disks(self):
        datastore = self._find_datastore()

        datastoreBrowser = datastore.browser
        vmDiskQuery = vim.host.DatastoreBrowser.VmDiskQuery()

        details = vim.host.DatastoreBrowser.VmDiskQuery.Details()
        details.capacityKb = True
        details.diskType = True
        details.hardwareVersion = True
        filters = vim.host.DatastoreBrowser.VmDiskQuery.Filter()
        diskType = []
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
        searchResultsTask = [datastoreBrowser.SearchDatastoreSubFolders_Task(
            datastorePath=self._flocker_volume_datastore_folder, searchSpec=searchSpec)]
        self._wait_for_tasks(searchResultsTask, self._si)
        searchResults = searchResultsTask[0].info.result
        logging.debug("_find_virtual_disks : " + str(searchResults))
        return searchResults

    def _list_vsphere_volumes(self):
        vms = self._find_all_vms()

        searchResults = self._find_virtual_disks()
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager

        vol_list = {}
        for result in searchResults:
            for file in result.file:
                logging.debug(str(file))
                volume_path = result.folderPath + file.path
                disk_uuid = virtualDiskManager.QueryVirtualDiskUuid(
                    name=volume_path, datacenter=self._dc)
                disk_uuid = self._normalize_uuid(disk_uuid)
                str_dataset_id = file.path
                str_dataset_id = str_dataset_id[:-5]
                logging.debug(str_dataset_id)
                dataset_id = uuid.UUID(str_dataset_id)
                logging.debug(dataset_id)

                volume = BlockDeviceVolume(
                    size=int(KiB(file.capacityKb).to_Byte().value),
                    dataset_id=dataset_id,
                    blockdevice_id=unicode(disk_uuid))
                vsphere_volume = VsphereBlockDeviceVolume(blockDeviceVolume=volume,
                                                          path=volume_path)
                for vm in vms:
                    if isinstance(vm, vim.VirtualMachine) and \
                            hasattr(vm.config, 'hardware'):
                        devices = vm.config.hardware.device
                        for device in devices:
                            if hasattr(device.backing, 'diskMode'):
                                device_disk_uuid = device.backing.uuid
                                device_disk_uuid = self._normalize_uuid(
                                    device_disk_uuid)
                                if device_disk_uuid == disk_uuid:
                                    volume = volume.set(
                                        'attached_to', unicode(vm._moId))
                                    vsphere_volume.blockDeviceVolume = volume
                                    vsphere_volume.vm = vm
                                    vsphere_volume.device = device
                                    break

                logging.debug(str(vsphere_volume))
                vol_list[unicode(disk_uuid)] = vsphere_volume

        logging.debug(str(vol_list))
        return vol_list

    def list_volumes(self):
        """
        List all the block devices available via the back end API.
        :raises ListVolumesFailure : If list volumes failed due to some error
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        try:
            logging.debug("starting list volumes")
            start_time = time.time()
            vol_list = self._list_vsphere_volumes()
            volumes = []
            for volume in vol_list.values():
                volumes.append(volume.blockDeviceVolume)

            logging.debug("vsphere list_volumes: " + str(volumes))
            logging.debug(start_time)
            logging.debug("Took %s seconds" % (time.time() - start_time))
            return volumes
        except Exception as e:
            logging.error("List volumes failed with error: " + str(e))
            raise ListVolumesFailure(e)

    def _find_all_disk_devices(self):
        output = check_output(["lsblk", "-d", "-o", "KNAME,TYPE"])
        logging.debug(output)
        lines = output.split("\n")
        logging.debug(lines)
        devices = []
        # remove 1st entry [KNAME, TYPE]
        del lines[0]
        # remove last empty entry
        del lines[-1]
        # extract only those devices where type = disk
        logging.debug(lines)
        for line in lines:
            data = line.split()
            if(data[1] == "disk"):
                devices.append("/dev/" + data[0])

        logging.debug(devices)
        return devices

    def get_device_path(self, blockdevice_id):
        """
        Return the device path that has been allocated to the block device on
        the host to which it is currently attached.
        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :raises GetDevicePathFailure: If get_device_path fails due to some error
        :returns: A ``FilePath`` for the device.
        """
        vsphere_volume = self._get_vsphere_blockdevice_volume(blockdevice_id)
        if vsphere_volume.blockDeviceVolume.attached_to is None:
            logging.error("Volume is not attached to a vm.")
            raise UnattachedVolume(blockdevice_id)

        try:
            devices = self._find_all_disk_devices()
            for device in devices:
                try:
                    output = check_output(["scsiinfo", "-s", device])
                    logging.debug(output)
                except:
                    logging.error("Error occured for scsiinfo -s " + device)
                    continue
                serial_id_line_index = output.find("'")
                if serial_id_line_index < 0:
                    logging.debug("No serial id found for device : " + device)
                    continue
                serial_id = output[serial_id_line_index:]
                logging.debug(serial_id)
                uid = self._normalize_uuid(serial_id)
                logging.debug(uid)
                logging.debug(blockdevice_id)
                if str(uid) == str(blockdevice_id):
                    logging.debug("Found device path : " + device)
                    return FilePath(device)
        except Exception as e:
            logging.error("Get device path failed with error : " + str(e))
            raise GetDevicePathFailure(e)

        raise GetDevicePathFailure("No device path found")


def vsphere_from_configuration(cluster_id, vc_ip, username, password,
                               datacenter_name, datastore_name, validate_cert):

    return VsphereBlockDeviceAPI(
        cluster_id=cluster_id,
        vc_ip=vc_ip,
        username=username,
        password=password,
        datacenter_name=datacenter_name,
        datastore_name=datastore_name,
        validate_cert=validate_cert
    )


def main():
    vs = vsphere_from_configuration(cluster_id='1',
                                    vc_ip="10.112.93.71",
                                    username=u'Administrator@vsphere.local',
                                    password=u'Admin!23',
                                    datacenter_name="Datacenter",
                                    datastore_name="vsanDatastore")
    volume = vs.create_volume(dataset_id=uuid.uuid4(), size=21474836480)
    vs.list_volumes()
    vm = vs.compute_instance_id()
    vs.attach_volume(blockdevice_id=volume.blockdevice_id, attach_to=vm)
    vs.list_volumes()
    vs.get_device_path(volume.blockdevice_id)
    # unicode('6000c29efe6df3d5ae7babe6ef9dea74'))
    # vs.compute_instance_id()
    vs.detach_volume(volume.blockdevice_id)
    vs.list_volumes()
    vs.destroy_volume(volume.blockdevice_id)
    # unicode('6000C2915c5df0c12ff0372b8bfb244f'))
    vs.list_volumes()
    # vs.get_device_path(unicode('6000c29efe6df3d5ae7babe6ef9dea74'))

if __name__ == '__main__':
    main()
