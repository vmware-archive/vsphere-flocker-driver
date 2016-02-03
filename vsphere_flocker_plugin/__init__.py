'''
Copyright 2015 VMware, Inc.  All rights reserved.  Licensed under the Apache v2 License.
'''
from flocker.node import BackendDescription, DeployerType
from vsphere_flocker_plugin.vsphere_blockdevice import vsphere_from_configuration


def api_factory(cluster_id, **kwargs):
    return vsphere_from_configuration(cluster_id=cluster_id,
                                      vc_ip=kwargs[u'vc_ip'],
                                      username=kwargs[u'username'],
                                      password=kwargs[u'password'],
                                      datacenter_name=kwargs[
                                          u'datacenter_name'],
                                      datastore_name=kwargs[u'datastore_name'])


FLOCKER_BACKEND = BackendDescription(
    name=u"vsphere_flocker_plugin",  # name isn't actually used for 3rd party plugins
    needs_reactor=False, needs_cluster_id=True,
    api_factory=api_factory, deployer_type=DeployerType.block)
