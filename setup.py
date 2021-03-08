import codecs
from pathlib import Path
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    file_path = Path(__file__).parent / filename
    return codecs.open(file_path, encoding="utf-8").read()


def find_meta(meta):
    """Extract __*meta*__ from META_FILE."""
    re_str = r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta)
    meta_match = re.search(re_str, META_FILE, re.M)
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError(f"Unable to find __{meta}__ string.")


PACKAGE_NAME = "dynamo-pandas"
META_FILE = read(Path(__file__).parent / PACKAGE_NAME.replace("-", "_") / "__init__.py")


setup(
    name=PACKAGE_NAME,
    version=find_meta("version"),
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
