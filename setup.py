from setuptools import setup, find_packages

setup(
    name='my_partition_tool',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'trino',
        'pandas',
        'sqlglot',
    ],
    entry_points={
        'console_scripts': [
            'partition-tool=my_partition_tool.cli:main'
        ]
    },
    author='Your Name',
    author_email='your.email@example.com',
    description='Adaptive partitioning tool for Trino instances based on usage and query logs.',
)
