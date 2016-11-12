# -*- coding: utf-8 -*-

import os
import site

from setuptools import find_packages
from setuptools import setup, Command

NAME = "anode"


# noinspection PyMethodMayBeStatic
class CleanCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        os.system("rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info {}/bin/{}".format(site.USER_BASE, NAME))


def readme():
    with open("README.rst") as f:
        return f.read()


setup(
    name=NAME,
    version="${project.version}".replace("-SNAPSHOT", ".dev0").replace("${project.version}", "0.0.0.dev0"),
    description=" asystem anode",
    long_description=readme(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 2.7",
        "Topic :: Other/Nonlisted Topic",
    ],
    keywords="asystem anode",
    url="https://github.com/ggear/asystem/tree/master/asystem-anode",
    author="Graham Gear",
    author_email="notmyemail@company.com",
    cmdclass={
        "clean": CleanCommand,
    },
    packages=find_packages(),
    install_requires=["twisted", "treq", "avro", "autobahn[twisted]", "klein"],
    tests_require=["mock"],
    test_suite="anode.test",
    setup_requires=["setuptools_trial"],
    entry_points={"console_scripts": ["anode=anode.anode:main"]},
    include_package_data=True,
    zip_safe=False
)
