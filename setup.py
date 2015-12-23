import os
from setuptools import setup

# https://caremad.io/2013/07/setup-vs-requirement/

with open('./requirements.txt') as r:
    # strip fixed version info from requirements file
    requirements = [line.split('=', 1)[0] for line in r]

setup(
    name='gdapi-python',
    version='0.5.2',
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
