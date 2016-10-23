import os
import time

from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(
    name='anode',
    version=os.getenv('VERSION', str(time.time())),
    description=' asystem anode',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Other/Nonlisted Topic',
    ],
    keywords='asystem anode',
    url='https://github.com/ggear/asystem/tree/master/asystem-anode',
    author='Graham Gear',
    author_email='notmyemail@company.com',
    packages=['anode'],
    install_requires=['twisted'],
    test_suite='nose.collector',
    tests_require=['mock', 'nose', 'nose-cover3'],
    entry_points={'console_scripts': ['anode=anode.anode:main']},
    include_package_data=True,
    zip_safe=False
)
