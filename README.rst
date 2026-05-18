Software Heritage - Object storage
==================================

Content-addressable object storage for the Software Heritage project.


Quick start
-----------

The easiest way to try the swh-objstorage object storage is to install it in a
virtualenv. Here, we will be using
`virtualenvwrapper <https://virtualenvwrapper.readthedocs.io>`_ but any virtual
env tool should work the same.

In the example below we will create a new objstorage using the
`pathslicer <https://docs.softwareheritage.org/devel/apidoc/swh.objstorage.html#module-swh.objstorage.objstorage_pathslicing>`_
backend.


.. code-block:: console

   ~/swh$ mkvirtualenv -p /usr/bin/python3 swh-objstorage
   [...]
   (swh-objstorage) ~/swh$ pip install swh.objstorage
   [...]
   (swh-objstorage) ~/swh$ cat >local.yml <<EOF
   objstorage:
     cls: pathslicing
     root: /tmp/objstorage
     slicing: 0:2/2:4/4:6
   EOF
   (swh-objstorage) ~/swh$ mkdir /tmp/objstorage
   (swh-objstorage) ~/swh$ swh objstorage -C local.yml rpc-serve -p 15003
   INFO:swh.core.config:Loading config file local.yml
   ======== Running on http://0.0.0.0:15003 ========
   (Press CTRL+C to quit)

Now we have an API listening on http://0.0.0.0:15003 we can use to store and
retrieve objects from. In an other terminal, you can import all the files from
a local directory in this objstorage:

.. code-block:: console

   ~/swh$ workon swh-objstorage
   (swh-objstorage) ~/swh$ cat >remote.yml <<EOF
   objstorage:
     cls: remote
     url: http://127.0.0.1:15003
   EOF
   (swh-objstorage) ~/swh$ swh objstorage -C remote.yml import .
   INFO:swh.core.config:Loading config file remote.yml
   Imported 1369 files for a volume of 722837 bytes in 2 seconds


Winery developer's check-list
-----------------------------

Working on Winery, the production backend, requires a slightly longer set-up.

First ensure your virtualenv contains the correct dependencies:

.. code-block:: console

    pip install -e .[winery]

Then create a postgres DB, called `winery`:

.. code-block:: console

    swh db create -d winery objstorage.backends.winery
    swh db init -d winery objstorage.backends.winery

Prepare a container folder:

.. code-block:: console

    mkdir /home/martin/objstores/winery

And set it in a configuration file we'll call `localwinery.yml`:

.. code-block:: yaml

  objstorage:
    cls: winery

    # boolean (false (default): allow writes, true: only allow reads)
    readonly: false

    shards:
      # integer: threshold in bytes above which shards get packed. Can be
      # overflowed by the max allowed object size.
      max_size: 100000000  # 100MB

      # float: timeout in seconds after which idle read-write shards get
      # released by the winery writer process
      rw_idle_timeout: 300

    database:
      # string: PostgreSQL connection string for the object index and read-write shards
      db: "dbname=winery"

      # string: PostgreSQL application name for connections (unset by default)
      application_name: localwinery

    shards_pool:
      ## Settings for the directory shards pool
      # Shards are stored in `{base_directory}/{pool_name}`
      type: directory
      base_directory: /home/martin/objstores/winery
      pool_name: shards

    packer:

      # Whether the winery writer should start packing shards immediately, or
      # defer to the standalone packer (default: true, the writer launches a
      # background packer process - in our case we prefer to launch a separate packer,
      # whose logs are easier to read when developing)
      pack_immediately: false

      # Whether the packer should create shards in the shard pool, or defer to
      # the pool manager (default: true, the packer creates images)
      create_images: true

      # Whether the packer should clean read-write shards from the database
      # immediately, or defer to the rw shard cleaner (default: true, the packer
      # cleans read-write shards immediately)
      clean_immediately: false


Note that this configuration implies to run a packer process separately.

Now you'll need a few terminal splits/tabs because we'll start 3 relevant services

.. code-block:: console

    # Main service (winery writer)  listens on 0.0.0.0:15003
    swh objstorage -C localwinery.yml rpc-serve -p  15003
    # Winery Packer Service
    swh objstorage -C localwinery.yml winery packer
    # optional, relevant later: RW Shard Cleaner
    swh objstorage -C localwinery.yml winery rw-shard-cleaner

To import contents we'll use the `swh objstorage import`, with the `remote.yml`
configuration created in the Quick Start section in order to use the RPC server we've
just started:

.. code-block:: console

    swh objstorage -C remote.yml import ~/swh-environment/


Test dependencies
-----------------

Some tests do require non-python dependencies to be installed on the machine:

- Ceph: the ``ceph`` executable can be used to run winery tests. When the ``ceph``
  binary is available, the winery tests will try to create a real ceph Rados
  Block Device (rbd) pool to run.

- Azurite: the ``azurite`` tool is needed for Azure backend tests. Since it's a
  npm package, you can install it using:

  .. code-block:: console

     ~/swh$ npm install -g azurite

  Note: you may want to configure your npm setup so it uses ``~/.local`` as
  prefix for global installations:

  .. code-block:: console

     ~/swh$ npm config set prefix '~/.local/'
