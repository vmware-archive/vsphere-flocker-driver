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
    version="1.0",
    description="VMware vSphere Storage Plugin for ClusterHQ/Flocker.",
    author="Pratik Gupta",
    author_email="pratik.gupta1088@gmail.com",
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='vsphere, backend, plugin, flocker, docker, python',
    url="https://github.com/vmware/vsphere-flocker-driver",
    install_requires=[
        "pyvmomi>=5.5.0-2014.1.1",
    ]
)
