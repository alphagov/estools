estools
=======

A set of commandline utilities for interacting with instances of the
elasticsearch_ search engine.

.. _elasticsearch: http://www.elasticsearch.org/

Installation
------------

``estools`` is available on PyPI_ and can be installed using pip_::

    $ pip install estools

.. _PyPI: http://pypi.python.org/pypi
.. _pip: http://www.pip-installer.org/
    
Usage
-----

Currently available tools within ``estools``

==================   ======================================================
Command              Description
==================   ======================================================
es-rotate            Rotate a set of daily indices (creates "current" 
                     aliases and deletes old indices)
es-river             Get, create, delete and compare rivers
es-template          Get, create, delete and compare index templates
==================   ======================================================

License
-------

``estools`` is released under the MIT license, a copy of which can be found in
``LICENSE``.
