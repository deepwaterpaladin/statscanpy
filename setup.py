from setuptools import setup

setup(
    name='statscanpy',
    version='0.1.5',
    description='Basic package for querying & downloading StatsCan data by table name.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=['pyspark','pandas']
)
