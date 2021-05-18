from setuptools import setup

setup(
    name="data-manager",
    version="0.1.0",
    packages=["datamanager", "datamanager.data_managers", "datamanager.interfaces", "datamanager.mixins", "datamanager.utils"],
    install_requires=["pyhocon", "pandas", "pyspark"],
)
