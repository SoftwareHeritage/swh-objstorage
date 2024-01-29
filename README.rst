Software Heritage - Object storage
==================================

Content-addressable object storage for the Software Heritage project.


Quick start
-----------

The easiest way to try the swh-objstorage object storage is to install it in a
virtualenv. Here, we will be using
[[https://virtualenvwrapper.readthedocs.io|virtualenvwrapper]]_ but any virtual
env tool should work the same.

In the example below we will create a new objstorage using the
[[https://docs.softwareheritage.org/devel/apidoc/swh.objstorage.html#module-swh.objstorage.objstorage_pathslicing|pathslicer]]
backend.


.. code-block:: shell

   ~/swh$ mkvirtualenv -p /usr/bin/python3 -i swh.objstorage swh-objstorage
   [...]
   (swh-objstorage) ~/swh$ cat >local.yml <<EOF
   objstorage:
     cls: pathslicing
     args:
       root: /tmp/objstorage
       slicing: 0:2/2:4/4:6
   EOF
   (swh-objstorage) ~/swh$ mkdir /tmp/objstorage
   (swh-objstorage) ~/swh$ swh-objstorage -C local.yml serve -p 15003
   INFO:swh.core.config:Loading config file local.yml
   ======== Running on http://0.0.0.0:15003 ========
   (Press CTRL+C to quit)

Now we have an API listening on http://0.0.0.0:15003 we can use to store and
retrieve objects from. I an other terminal:

.. code-block:: shell

   ~/swh$ workon swh-objstorage
   (swh-objstorage) ~/swh$ cat >remote.yml <<EOF
   objstorage:
     cls: remote
     args:
       url: http://127.0.0.1:15003
   EOF
   (swh-objstorage) ~/swh$ swh-objstorage -C remote.yml import .
   INFO:swh.core.config:Loading config file remote.yml
   Imported 1369 files for a volume of 722837 bytes in 2 seconds


Test dependencies
-----------------

Some tests do require non-python dependencies to be installed on the machine:

- Ceph: the `ceph` executable can be used to run winery tests. When the ceph
  binary is available, the winery tests will try to create a real ceph Rados
  Block Device (rbd) pool to run.

- Azurite: the ``azurite`` tool is needed for Azure backend tests. Since it's a
  npm package, you can install it using:

  .. code-block:: console

     npm install -g azurite

  Note: you may want to configure your npm setup so it uses ``~/.local`` as
  prefix for global installations:

  .. code-block:: console

     npm config set prefix '~/.local/'
