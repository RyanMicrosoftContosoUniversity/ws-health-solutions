"""
note that notebooktuls and pyspark are removed from install_requires
this is because they cause issues with the packaging

Therefore this will only work in a Fabric environment
"""
from setuptools import setup, find_packages

# Open the README file.
with open(file="README.md", mode="r") as fh:
    long_description = fh.read()

setup(
    name='fabric_utils',
    version='0.0.2',
    install_requires=[
        'msal',
        'azure-identity',
        'azure-keyvault-secrets',
        'requests',
        'pyodbc',
        'semantic-link-labs'

    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            # Define command-line scripts here, e.g.,
            # 'metadata-scan=scripts.metadata_scan:main',
        ],
    },
    include_package_data=True,
    description='A framework for fabric utilities',
    author='RH',
    author_email='RH@gmail.com',
    url='https://github.com/RyanMicrosoftContosoUniversity/ws-health-solutions',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.11',
)