from setuptools import setup

setup(
    name='statscanpy',
    version='0.1.1',
    description='Basic package for accessing StatsCan data.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=['pyspark','pandas']
)
