'''
Copyright 2015 VMware, Inc.  All rights reserved.  Licensed under the Apache v2 License.
'''
from setuptools import setup

setup(
    name="vSphere Flocker driver",
    packages=[
        "vsphere_flocker_plugin"
    ],
    package_data={
        "vsphere_flocker_plugin": ["config/*"],
    },
    version="0.1",
    description="VMware vSphere Storage Plugin for ClusterHQ/Flocker.",
    author="Pratik Gupta",
    author_email="pratik.gupta1088@gmail.com",
    url="https://github.com/vmware/vsphere-flocker-driver",
    install_requires=[
        "pyvmomi>=5.5",
    ]
)
