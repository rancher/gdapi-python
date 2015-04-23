import os
from setuptools import setup

with open('./requirements.txt') as r:
    requirements = [line for line in r]

setup(
    name='gdapi-python',
    version='0.4.0',
    py_modules=['gdapi'],
    url='https://github.com/godaddy/gdapi-python',
    license='MIT Style',
    author='Darren Shepherd',
    author_email='darren.s.shepherd@gmail.com',
    description='Python API and CLI for GDAPI standard',
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'gdapi = gdapi:_main'
        ]
    }
)
