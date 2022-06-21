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
        os.system("rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg* {}/bin/{} {}/.eggs".format(
            site.USER_BASE, NAME, os.path.dirname(os.path.realpath(__file__))))


setup(
    name=NAME,
    version="${project.version}".replace("-SNAPSHOT", ".dev0").replace("${project.version}", "0.0.0.dev0"),
    description=" asystem anode",
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
    package_data={"": ["*.properties"]},
    install_requires=[
        "twisted==16.5.0",
        "twisted_s3==0.2.2",
        "twisted-mqtt==0.3.6",
        "autobahn==0.16.1",
        "klein==15.3.1",
        "treq==17.8.0",
        "avro==1.7.6",
        "cycler==0.10.0",
        "requests==2.18.4",
        "ilio==0.3.0",
        "dill==0.2.6",
        "pandas==0.20.1",
        "numpy==1.22.0",
        "mpmath==1.0.0",
        "scipy==0.19.0",
        "scikit-learn==0.18.1",
        "matplotlib==2.0.0",
        "astral==1.9.2",
        "pyyaml==3.12",
        "xmltodict==0.11.0",
        "pathlib2==2.3.2",
        "python-dateutil==2.6.1",
        "attrs==18.1.0",
        "pyasn1==0.4.8",
        "pymysql==0.9.3",
    ],
    tests_require=["mock"],
    test_suite="anode.test",
    setup_requires=["setuptools_trial"],
    entry_points={"console_scripts": ["anode=anode.anode:main"]},
    include_package_data=True,
    zip_safe=False
)
