from setuptools import find_packages
from setuptools import setup

from dynamo_pandas import __version__

setup(
    name="dynamo-pandas",
    version=__version__,
    author="Julien de la Bruère-Terreault",
    author_email="drgfreeman@tuta.io",
    maintainer="Julien de la Bruère-Terreault",
    maintainer_email="drgfreeman@tuta.io",
    license="MIT",
    url="https://github.com/DrGFreeman/dynamo-pandas",
    description="Make working with pandas dataframe and AWS DynamoDB easy.",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=["boto3", "pandas>=1"],
)
