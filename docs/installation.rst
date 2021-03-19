Installation
============

.. toctree::
    :maxdepth: 3
    :caption: Contents:


Install ``dynamo-pandas`` from `PyPI <https://pypi.org/project/dynamo-pandas/>`_ using pip:

.. code-block:: console

    $ pip install dynamo-pandas


This will install the package and its dependencies except for `boto3` which is not installed by default to avoid unnecessary installation when building Lambda layers.

To include `boto3` as part of the installation, add the `boto3` "extra" this way:

.. code-block:: console

    $ python -m pip install dynamo-pandas[boto3]


Requirements
------------

``dynamo-pandas`` has the following requirements:

* ``python`` >= 3.7
* ``pandas`` >= 1
* ``boto3``