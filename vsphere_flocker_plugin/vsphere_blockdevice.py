'''
Copyright 2015 VMware, Inc.  All rights reserved.  Licensed under the Apache v2 License.
'''
from flocker.node.agents.blockdevice import (
    VolumeException, AlreadyAttachedVolume,
    UnknownVolume, UnattachedVolume,
    IBlockDeviceAPI, BlockDeviceVolume
)
from pyVmomi import vim, vmodl
import atexit
from pyVim.connect import SmartConnect, Disconnect
from twisted.python.filepath import FilePath
from zope.interface import implementer
from subprocess import check_output
from bitmath import Byte, GiB, KiB
import netifaces
import logging
import uuid
import time
import ssl

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
        return "<VsphereBlockDeviceVolume {}, {}, {}, {}>".format(self.blockDeviceVolume,
                                                                  self.path, self.vm,
                                                                  self.device)


@implementer(IBlockDeviceAPI)
class VsphereBlockDeviceAPI(object):

    """
    A ``IBlockDeviceAPI`` which creates volumes (vmdks) with vsphere backend.
    """

    def __init__(self, cluster_id, vc_ip, username, password,
                 datacenter_name, datastore_name, ssl_verify_cert,
                 ssl_key_file, ssl_cert_file, ssl_thumbprint):

        self._cluster_id = cluster_id
        self._vc_ip = vc_ip
        self._username = username
        self._password = password
        self._datacenter_name = datacenter_name
        self._datastore_name = datastore_name
        # SSL Cert verification pyvmomi 6.0+
        self._ssl_verify_cert = ssl_verify_cert
        self._ssl_key_file = ssl_key_file
        self._ssl_cert_file = ssl_cert_file
        self._ssl_thumbprint = ssl_thumbprint
        logging.debug("Initializing {}; {}; {}; {}; {}".format(self._cluster_id, self._vc_ip,
                                                               self._username, self._datacenter_name,
                                                               self._datastore_name))

        self._connect()

        content = self._si.RetrieveContent()
        for childEntity in content.rootFolder.childEntity:
            if childEntity.name == self._datacenter_name:
                self._dc = childEntity
                break

        logging.debug("Datacenter: {} ({})".format(self._dc.name, str(self._dc)))

        self._flocker_volume_datastore_folder = '[' + \
            self._datastore_name + '] FLOCKER/'
        logging.debug("Datastore folder: {}".format(self._flocker_volume_datastore_folder))

    def _connect(self):

        try:
            # Connect to VC
            # si - the root object of inventory
            logging.debug('Connecting to {} with SSL verification {}'.format(self._vc_ip,
                                                                             self._ssl_verify_cert))

            if self._ssl_verify_cert and hasattr(ssl, 'SSLContext'):
                logging.debug('SSL Verification selected '
                              'cert={}; key={}; thumbprint={}'.format(self._ssl_cert_file,
                                                                      self._ssl_key_file,
                                                                      self._ssl_thumbprint))
                self._si = SmartConnect(host=self._vc_ip, port=443,
                                        user=self._username, pwd=self._password,
                                        keyFile=self._ssl_key_file,
                                        certFile=self._ssl_cert_file,
                                        thumbprint=self._ssl_thumbprint)
            elif hasattr(ssl, 'SSLContext'):
                logging.debug('Creating unverified context with TLSv1 and no Cert')
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
                ctx.verify_mode = ssl.CERT_NONE
                self._si = SmartConnect(host=self._vc_ip, port=443,
                                        user=self._username, pwd=self._password,
                                        sslContext=ctx)
            else:
                logging.warning('Could not create unverified context. '
                                'Please, make sure you have pyVmomi 5.5.0-2014.1 installed in '
                                'order to make a successful vCenter connection.')
                self._si = SmartConnect(host=self._vc_ip, port=443,
                                        user=self._username, pwd=self._password)
                atexit.register(Disconnect, self._si)

        except Exception as e:
            logging.error("Connection to VC failed with error : {}".format(e))
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
            logging.debug("Getting local IPs: {}".format([addr for addr in localIps]))
            for localIp in localIps:
                vms = searchIndex.FindAllByIp(
                    datacenter=self._dc, ip=localIp, vmSearch=True)
                logging.debug("Getting local VMs: {}".format(', '.join(['{} ({})'.format(vm.name, vm._moId) for vm in vms])))
                if vms:
                    logging.debug("Local VM instance id: {}".format(vms[0]._moId))
                    return vms[0]._moId
        except Exception as e:
            logging.error("Could not find vm because of error : " + str(e))

        logging.error("This vm instance is not found in VC")
        raise VmNotFound("This vm instance is not found in VC")

    def allocation_unit(self):
        """
        Return allocation unit
        """
        logging.debug("vSphere allocation unit: " +
                      str(int(GiB(4).to_Byte().value)))
        return int(GiB(4).to_Byte().value)

    def _normalize_uuid(self, uuid):
        """
        Normalizes the input uuid to lower-case string without any white space or '-'
        """
        uuid = uuid.translate(None, " -\n'")
        uuid = uuid.lower()
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
            # Using vim enums
            fileBackedVirtualDiskSpec.adapterType = vim.VirtualDiskManager.VirtualDiskAdapterType.lsiLogic
            fileBackedVirtualDiskSpec.diskType = vim.VirtualDiskManager.VirtualDiskType.thin
            logging.debug("Creating VMDK capacityKb: {}; adapterType: {}; "
                          "diskType: {}".format(str(fileBackedVirtualDiskSpec.capacityKb),
                                                fileBackedVirtualDiskSpec.adapterType,
                                                fileBackedVirtualDiskSpec.diskType))

            path_name = self._flocker_volume_datastore_folder + \
                str(dataset_id) + ".vmdk"

            new_disk = [virtualDiskManager.CreateVirtualDisk_Task(
                name=path_name, datacenter=self._dc, spec=fileBackedVirtualDiskSpec)]
            self._wait_for_tasks(new_disk, self._si)

            logging.debug("VMDK {} created successfully".format(path_name))
            uuid = virtualDiskManager.QueryVirtualDiskUuid(
                name=path_name, datacenter=self._dc)
            logging.debug("VMDK {} UUID: {}".format(path_name, str(uuid)))
        except Exception as e:
            logging.error(
                "Cannot create volume because of exception : " + str(e))
            raise VolumeCreationFailure(e)
        # normalizing uuid
        uuid = self._normalize_uuid(uuid)
        logging.debug("VMDK {} UUID normalized {}".format(path_name, uuid))
        # creating flocker block device volume
        volume = BlockDeviceVolume(
            size=size,
            dataset_id=dataset_id,
            blockdevice_id=unicode(uuid))
        logging.debug("vSphere Block Device Volume ID {}".format(volume.blockdevice_id))
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
                                logging.debug("{} ".format(task.info.result))
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
        for vm in self._get_all_vms():
            if unicode(vm['moref']._moId) == moid:
                return vm['moref']

    def _get_all_vms(self):
        """ Gets list of VMs in current vCenter instance
        :return: list of vms
        """
        start_time = time.time()
        vms = self._get_properties_from_vc([vim.VirtualMachine],
                                           ['name', 'config.hardware'],
                                           vim.VirtualMachine)
        logging.debug("Took {} seconds".format(time.time() - start_time))
        return vms

    def _get_properties_from_vc(self, viewType, props, specType):
        """ Obtains a list of specific properties for a
        particular Managed Object Reference data object.
        :param content: ServiceInstance Managed Object
        :param viewType: Type of Managed Object Reference
                         that should populate the View
        :param props: A list of properties that should be
                     retrieved for the entity
        :param specType: Type of Managed Object Reference
                         that should be used for the Property
                         Specification
        :return:
        """
        content = self._si.RetrieveContent()
        # Build a view and get basic properties for all Virtual Machines
        objView = content.viewManager.CreateContainerView(content.rootFolder, viewType, True)
        tSpec = vim.PropertyCollector.TraversalSpec(name='tSpecName',
                                                    path='view',
                                                    skip=False,
                                                    type=vim.view.ContainerView)
        pSpec = vim.PropertyCollector.PropertySpec(all=False,
                                                   pathSet=props,
                                                   type=specType)
        oSpec = vim.PropertyCollector.ObjectSpec(obj=objView,
                                                 selectSet=[tSpec],
                                                 skip=False)
        pfSpec = vim.PropertyCollector.FilterSpec(objectSet=[oSpec],
                                                  propSet=[pSpec],
                                                  reportMissingObjectsInResults=False)
        retOptions = vim.PropertyCollector.RetrieveOptions()
        totalProps = []
        retProps = content.propertyCollector.RetrievePropertiesEx(specSet=[pfSpec],
                                                                  options=retOptions)
        totalProps += retProps.objects
        while retProps.token:
            retProps = content.propertyCollector.ContinueRetrievePropertiesEx(token=retProps.token)
            totalProps += retProps.objects
        objView.Destroy()
        # Turn the output in retProps into a usable dictionary of values
        gpOutput = []
        for eachProp in totalProps:
            propDic = {}
            for prop in eachProp.propSet:
                propDic[prop.name] = prop.val
            propDic['moref'] = eachProp.obj
            gpOutput.append(propDic)
        return gpOutput

    def _get_vsphere_blockdevice_volume(self, blockdevice_id):
        logging.debug("Looking for {}", blockdevice_id)
        vol_list = self._list_vsphere_volumes()
        if blockdevice_id not in vol_list.keys():
            logging.error(
                "Volume not found for BlockDevice ID {} ".format(blockdevice_id))
            raise UnknownVolume(blockdevice_id)
        return vol_list[blockdevice_id]

    def _delete_vmdk(self, vsphere_volume, blockdevice_id):
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager
        tasks = [virtualDiskManager.DeleteVirtualDisk_Task(
            name=vsphere_volume.path, datacenter=self._dc)]
        self._wait_for_tasks(tasks, self._si)
        logging.debug("VMDK {} with Block device Volume "
                      "{} deleted successfully".format(vsphere_volume,
                                                       blockdevice_id))

    def _create_new_scsi_controller(self, si, vm_obj):
        """ Creates SCSI controller
        :param si: Service Instance Object
        :type si: vim.ServiceInstance
        :param vm_obj: Virtual Machine Object
        :param vm_obj: vim.VirtualMachine
        :return:
        """
        scsi_controller = vim.vm.device.VirtualLsiLogicController()
        scsi_controller.sharedBus = vim.vm.device.VirtualSCSIController.Sharing.noSharing
        scsi_controller.key = 1  # since no scsi controllers exist
        scsi_controller_spec = vim.vm.device.VirtualDeviceSpec()
        scsi_controller_spec.device = scsi_controller
        scsi_controller_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        # Adding new scsi controller to device_change
        spec = vim.vm.ConfigSpec()
        spec.deviceChange = [scsi_controller_spec]
        self.wait_for_tasks(si, [vm_obj.ReconfigVM_Task(spec=spec)])
        return True

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
            self._delete_vmdk(vsphere_volume, blockdevice_id)
        except Exception as e:
            logging.error("Destroy volume failed due to error " + str(e))
            raise VolumeDestroyFailure(e)

    def _attach_vmdk(self, vm, vsphere_volume):
        vm_device = vm.config.hardware.device
        vm_uuid = vm.config.instanceUuid
        vm_name = vm.name
        logging.debug('vSphere volume {} will be attached to ({})'.format(vsphere_volume, vm_name, vm_uuid))
        controller_to_use = None
        available_units = [0]
        # setting up a baseline list
        base_unit_range = range(0, 16)  # disks per controller
        base_unit_range.remove(7)  # unit_number 7 reserved for scsi controller
        # get relationship controller - disk
        devices = []
        for dev in vm_device:
            if isinstance(dev, vim.vm.device.VirtualSCSIController):
                devices.append({'controller': dev,
                                'devices': [d.unitNumber for d in vm_device for k in dev.device if k == d.key]})
        # getting any available slots in existing controllers
        for controller in devices:
            available = set(base_unit_range) - set(controller['devices'])
            if available:
                controller_to_use = controller['controller']
                available_units = list(available)
                break
        # Creates controller if none available
        if not controller_to_use:
            # Either there are no available slots or
            # does not have SCSI controller
            self._create_new_scsi_controller(self._si, vm)
            vm = self._si.content.searchIndex.FindByUuid(None, vm_uuid, True, True)
            controller_to_use = [dev for dev in vm.config.hardware.device
                                 if isinstance(dev, vim.vm.device.VirtualSCSIController)][0]
            logging.debug('New SCSI controller created: {}'.format(controller_to_use.deviceInfo.label))

        logging.debug('SCSI controller to use: {}'.format(controller_to_use.deviceInfo.label))

        new_disk_kb = int(Byte(vsphere_volume.blockDeviceVolume.size).to_KiB().value)
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        # device config
        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.unitNumber = available_units[0]
        disk_spec.device.capacityInKB = new_disk_kb
        disk_spec.device.controllerKey = controller_to_use.key
        # device backing info
        disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        disk_spec.device.backing.thinProvisioned = True
        disk_spec.device.backing.diskMode = vim.vm.device.VirtualDiskOption.DiskMode.persistent
        disk_spec.device.backing.fileName = vsphere_volume.path
        # Submitting config spec
        spec = vim.vm.ConfigSpec()
        spec.deviceChange = [disk_spec]
        tasks = [vm.ReconfigVM_Task(spec=spec)]
        self._wait_for_tasks(tasks, self._si)
        # vsphere volume
        volume = vsphere_volume.blockDeviceVolume
        attached_volume = volume.set('attached_to', unicode(vm._moId))
        logging.debug('vSphere volume {} attached to {} successfully'.format(vsphere_volume, vm))
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
        logging.debug("Attaching {} to {}".format(blockdevice_id,
                                                  attach_to))
        vm = self._find_vm(attach_to)
        try:
            if vm is None:
                raise UnknownVm("VM not found {}".format(attach_to))

            vsphere_volume = self._get_vsphere_blockdevice_volume(blockdevice_id)
            if vsphere_volume.blockDeviceVolume.attached_to is not None:
                logging.error("Volume is attached to a vm so cannot attach.")
                raise AlreadyAttachedVolume(blockdevice_id)

            try:
                attached_volume = self._attach_vmdk(vm, vsphere_volume)
            except Exception as e:
                logging.error("Cannot attach volume {} to {} "
                              "because of exception: {}".format(blockdevice_id,
                                                                attach_to, e))
                raise VolumeAttachFailure(e)

            logging.debug("Volume attached to {}".format(attached_volume.attached_to))
            logging.debug("Block Device {} will be attached to {} {}".format(blockdevice_id,
                                                                             attach_to,
                                                                             attached_volume))
            logging.debug("Rescanning scsi bus for attached disk")
            self._rescan_scsi()
            return attached_volume
        except Exception, ex:
            logging.error('An error occurred attaching volume {} to {}: {}'.format(blockdevice_id,
                                                                                   attach_to,
                                                                                   ex))
            raise VolumeAttachFailure(ex)

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
            logging.error("Detach volume {} failed with error: {}".format(detached_volume, e))
            raise VolumeDetachFailure(e)

        self._rescan_scsi()
        logging.debug("Volume {} successfully detached." .format(blockdevice_id))

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
        logging.debug("Virtual Disks: {}".format(", ".join([str(sr.file) for sr in searchResults])))
        return searchResults

    def _list_vsphere_volumes(self):
        """ Lists vsphere volumes in configured
            datastore only.
        :return:
        """
        datastore = self._find_datastore()
        vms = datastore.vm
        logging.debug('Found {} VMs'.format(len(vms)))
        searchResults = self._find_virtual_disks()
        content = self._si.RetrieveContent()
        virtualDiskManager = content.virtualDiskManager

        vol_list = {}
        for result in searchResults:
            for file in result.file:
                volume_path = result.folderPath + file.path
                disk_uuid = virtualDiskManager.QueryVirtualDiskUuid(
                    name=volume_path, datacenter=self._dc)
                disk_uuid = self._normalize_uuid(disk_uuid)
                str_dataset_id = file.path
                str_dataset_id = str_dataset_id[:-5]
                # logging.debug(str_dataset_id)
                dataset_id = uuid.UUID(str_dataset_id)
                # logging.debug(dataset_id)
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
                vol_list[unicode(disk_uuid)] = vsphere_volume

        logging.debug("Found {} vSphere volumes: {}".format(len(vol_list),
                                                            '.'.join(vol_list.keys())))
        return vol_list

    def list_volumes(self):
        """
        List all the block devices available via the back end API.
        :raises ListVolumesFailure : If list volumes failed due to some error
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        try:
            logging.debug("List Volumes started")
            start_time = time.time()
            vol_list = self._list_vsphere_volumes()
            volumes = []
            for volume in vol_list.values():
                volumes.append(volume.blockDeviceVolume)

            logging.debug("vSphere Volumes found {}: {}".format(len(volumes),
                                                                str(volumes)))
            logging.debug("List volumes finished. Took {} seconds".format(time.time() - start_time))
            return volumes
        except Exception as e:
            logging.error("List volumes failed with error: " + str(e))
            raise ListVolumesFailure(e)

    def _find_all_disk_devices(self):
        logging.debug("Executing lsblk -d -o KNAME,TYPE")
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
            if data[1] == "disk":
                devices.append("/dev/" + data[0])

        logging.debug('Devices found: {}'.format('.'.join(devices)))
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
                    logging.debug("Executing scsiinfo -s {} ".format(device))
                    output = check_output(["scsiinfo", "-s", device])
                    logging.debug(output)
                except Exception, ex:
                    logging.error("Error occured for scsiinfo -s {}: {}".format(device, ex))
                    continue
                serial_id_line_index = output.find("'")
                if serial_id_line_index < 0:
                    logging.debug("No serial id found for device: {}".format(device))
                    continue
                serial_id = output[serial_id_line_index:]
                logging.debug(serial_id)
                uid = self._normalize_uuid(serial_id)
                logging.debug(uid)
                logging.debug(blockdevice_id)
                if str(uid) == str(blockdevice_id):
                    logging.debug("Found device path {}".format(device))
                    return FilePath(device)
        except Exception, ex:
            logging.error("Get device path failed with error: {}".format(ex))
            raise GetDevicePathFailure(ex)

        raise GetDevicePathFailure("No device path found")


def vsphere_from_configuration(cluster_id, vc_ip, username, password,
                               datacenter_name, datastore_name,
                               ssl_verify_cert, ssl_key_file,
                               ssl_cert_file, ssl_thumbprint):

    return VsphereBlockDeviceAPI(
        cluster_id=cluster_id,
        vc_ip=vc_ip,
        username=username,
        password=password,
        datacenter_name=datacenter_name,
        datastore_name=datastore_name,
        ssl_verify_cert=ssl_verify_cert,
        ssl_key_file=ssl_key_file,
        ssl_cert_file=ssl_cert_file,
        ssl_thumbprint=ssl_thumbprint
    )


def main():
    vs = vsphere_from_configuration(cluster_id='1',
                                    vc_ip="10.112.93.71",
                                    username=u'Administrator@vsphere.local',
                                    password=u'Admin!23',
                                    datacenter_name="Datacenter",
                                    datastore_name="vsanDatastore",
                                    ssl_verify_cert=False,
                                    ssl_key_file='',
                                    ssl_cert_file='',
                                    ssl_thumbprint='')
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
