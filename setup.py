from setuptools import setup

setup(
    name="photo_organizer",
    version='0.1.0',
    author="Jouni Kallunki",
    author_email="",
    description=("Tool for organizing and backuping photos"),
    entry_points={
        'console_scripts': ['photo-organizer = main:program.run']
    },
    install_requires=[
        'appdirs==1.4.3',
        'boto3==1.4.3',
        'botocore==1.4.92',
        'docutils==0.13.1',
        'ExifRead==2.1.2',
        'futures==3.0.5',
        'hachoir-core==1.3.3',
        'hachoir-metadata==1.3.3',
        'hachoir-parser==1.3.4',
        'invoke==0.20.4',
        'jmespath==0.9.0',
        'packaging==16.8',
        'pyparsing==2.2.0',
        'python-dateutil==2.6.0',
        's3transfer==0.1.10',
        'six==1.10.0'
    ],
    py_modules=['archive_to_glacier', 'glacier_inventory', 'rename_photos', 'tasks', 'main']
)
