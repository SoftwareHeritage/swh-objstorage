.. _swh-objstorage-winery:

Winery backend
==============

The Winery backend is an swh-objstorage backend that implements the `Ceph based
object storage architecture
<https://wiki.softwareheritage.org/wiki/A_practical_approach_to_efficiently_store_100_billions_small_objects_in_Ceph>`__.

Design specifications
---------------------

The idea of the Winery backend is to provide an object storage capable of
storing with decent performances the workload of the |swh| objstorage, that is
(at time of writing, aka 08/2025):

- about 2PB of total storage capacity
- 25B unique objects, for witch:
  - 75% are smaller than 16KB
  - 50% are smaller than 4KB
- capable of accepting many concurrent write operations

The desired design specs for winery are:

- Capable of handling 10PB storage capacity with commodity hardware
- Capable of storgin 100 billion (mostly small) objects
- At least 3,000 object/s and 100MB/s of write capacity
- At least 3,000 object/s and 100MB/s of read capacity
- Immune to space amplification
- Getting the first byte of any object never takes longer than 100ms.
- Objects can be enumerated in bulk, at least one million at a time.
- Mirroring the content of the Software Heritage archive can be done in bulk,
  at least one million objects at a time.

Architecture
------------

In a nutshell, objects are written to a number of dedicated tables in a
database used by a fixed number of services/machines (the Write Storage) that
can vary to control the write throughput. When a threshold is reached (e.g.
100GB) on a table, all objects in this table are put together in container (a
Shard), and moved to a readonly storage that keeps expanding over time.

After a successful write, a unique identifier (the Object ID) is returned to
the client. It can be used to read the object back from the readonly storage.
Reads scale out because the unique identifiers of the objects embed the name of
the container (the Shard UUID). Writes also scales out because the database
table in which the object is written is chosen randomly. This is the Layer 0.

Since clients of the swh-objstorage API cannot keep track of the name of the
container, it rely on an index mapping API that maps all known objects
signatures (the Object HASH below) to the name of the container where they can
be found. Although this index prevents scaling out writes, the readonly storage
can still scale out by multiplying copies of the index as needed. This is the
Layer 1.


.. thumbnail:: ./_images/winery-architecture.svg

   General view of a Winery based swh-objstorage.


Writer storage
~~~~~~~~~~~~~~

This is the part of the winery objstorage backend responsible for writing new
objects. Each new objstorage writer process will have create a new rw-shard by
the mean of a new table in the database. As such, if a gunicorn server handling
this writer storage is started with N workers, there will be N shard tables
created in the database, thus allowing to handle N concurrent write requests.

When a write request is received, it is routed to one of the writer worker by
gunicorn, thus toward one of the open rw shards. It's identifier is added in
the main shards index table and its content is added in the dedicated shard
table.

When a rw-shard is considered as full (when the total volume of objects stored
in this rw-shard reached the ``max_shard_size`` limit -- typically 100BG), the
shard is closed and does not access new objects.

Then a packing process will come and dump all the objects from the database
table into a shard file stored on the shard backend storage -- typically a Ceph
cluster, either using RBD volumes directly, of saving shard files on a ceph-fs
mounted shared filesystem. The shard entry in the shards table is marked as
``readonly`` and the dedicated table can then be destroyed.

Reader storage
~~~~~~~~~~~~~~

This is the part of the winery objstorage backend responsible for reading
objects.

There are 2 possible cases: the required object can be stored in a ro-shard (so
in a shard-file stored in the shard file storage), or in a rw-shard (thus in
one of the open shard tables in the database) when the object has not yet made
its way all the way to a ro-shard file.

When a object is requested, the shards index is queried to retrieve the
identifier of the shard in which this object is. This id is used to retrieve
the name and state of this shard. Depending on the state of the shard, the
object content will then be retrieved either from the shard table (if the shard
is not yet marked as ``readonly``), or retrieved from the shard file otherwise.


Shard file backend storage
~~~~~~~~~~~~~~~~~~~~~~~~~~

Shard files are using a custom but simple binary file format to pack together a
number of objects. It uses a cpmh based index system to make very fast to query
for a particular object (from the object's hash -- sha256). It is using the
:ref:`swh-shard` library to create, read and manipulate these files.

In order to support the creation, storage and replication of 10k+ shard files,
a clustered and safe storage solution must be used as backend.

Initially, Winery only supported Ceph RBD volumesas backend, but it now
supports writing these shard files in any shared storage space (CephFS, NFS
etc).


IO Throttling
--------------

Ceph (Pacific) implements IO QoS in librbd but it is only effective within a
single process, not cluster wide. The preliminary benchmarks showed that
accumulated read and write throughput must be throttled client side to prevent
performance degradation (slower throughput and increased latency).

Table are created in a PostgreSQL database dedicated to throttling, so
independent processes performing I/O against the Ceph cluster can synchronize
with each other and control their accumulated throughput for reads and writes.
Workers creates a row in the read and write tables and update them every minute
with their current read and write throughput, in bytes per second. They also
query all rows to figure out the current accumulated bandwidth.

If the current accumulated bandwidth is above the maximum desired speed for N
active workers, the process will reduce its throughput to use a maximum of 1/N
of the maximum desired speed. For instance, if the current accumulated usage is
above 100MB/s and there are 10 workers, the process will reduce its own speed
to 10MB/s. After the 10 workers independently do the same, each of them will
share 1/10 of the bandwidth.

Implementation notes
--------------------

:py:mod:`swh.objstorage.backends.winery.sharedbase` contains the global
    objstorage index implementation, which associates every object id
    (currently, the SHA256 of the content) to the shard it contains. The list
    of shards is stored in a table, associating them with a numeric id to save
    space, and their current
    :py:class:`swh.objstorage.backends.winery.sharedbase.ShardState`. The name
    of the shard is used to create a table (for write shards) or a RBD image
    (for read shards).

:py:mod:`swh.objstorage.backends.winery.roshard` handles read-only shard
    management: classes handling the lifecycle of the shards pool, the
    :py:class:`swh.objstorage.backends.winery.roshard.ROShardCreator`, as well
    as :py:class:`swh.objstorage.backends.winery.roshard.ROShard`, a thin layer
    on top of :py:mod:`swh.shard` used to access the objects stored inside a
    read-only shard.

:py:mod:`swh.objstorage.backends.winery.rwshard` handles the database-backed
    write shards for all their lifecycle.

:py:class:`swh.objstorage.backends.winery.objstorage.WineryObjStorage` is the
    main entry point compatible with the :py:mod:`swh.objstorage` interface. It
    is a thin layer backed by a
    :py:class:`swh.objstorage.backends.winery.objstorage.WineryWriter` for
    writes, and a
    :py:class:`swh.objstorage.backends.winery.objstorage.WineryReader` for
    read-only accesses.

:py:class:`swh.objstorage.backends.winery.objstorage.WineryReader` performs
    read-only actions on both read-only shards and write shards. It will first
    determine the kind of shard the object belongs to by looking it up in the
    global index. If it is a read-only Shard, it will lookup the object using
    :py:class:`swh.objstorage.backends.winery.roshard.ROShard`, backed by the
    RBD or directory-based shards pool. If it is a write shard, it will lookup
    the object using the
    :py:class:`swh.objstorage.backends.winery.rwshard.RWShard`, ultimately
    using a PostgreSQL table.

All :py:class:`swh.objstorage.backends.winery.objstorage.WineryWriter`
operations are idempotent so they can be resumed in case they fail. When a
:py:class:`swh.objstorage.backends.winery.objstorage.WineryWriter` is
instantiated, it will either:

* Find a write shard (i.e. a table) that is not locked by another instance by
  looking up the list of shards or,
* Create a new write shard by creating a new table

and it will lock the write Shard and own it so no other instance tries to write
to it. Locking is done transactionally by setting a locker id in the shards
index, when the
:py:class:`swh.objstorage.backends.winery.objstorage.WineryWriter` process dies
unexpectedly, these entries need to be manually cleaned up.

Writing a new object writes its identifier in the index table, and its contents
in the shard table, within the same transaction.

When the cumulative size of all objects within a Write Shard exceeds a
threshold, it is set to be in the `full` state. All objects it contains can be
read from it by any
:py:class:`swh.objstorage.backends.winery.objstorage.WineryReader` but no new
object will be added to it. When `pack_immediately` is set, a process is
spawned and is tasked to transform the `full` shard into a Read Shard using the
:py:class:`swh.objstorage.backends.winery.objstorage.Packer` class. Should the
packing process fail for any reason, a cron job will restart it when it finds
Write Shards that are both in the `packing` state and not locked by any
process. Packing is done by enumerating all the records from the Write Shard
database and writing them into a Read Shard by the same name. Incomplete Read
Shards will never be used by
:py:class:`swh.objstorage.backends.winery.objstorage.WineryReader` because the
global index will direct it to use the Write Shard instead. Once the packing
completes, the state of the shard is modified to be `packed`, and from that
point on the :py:class:`swh.objstorage.backends.winery.objstorage.WineryReader`
will only use the Read Shard to find the objects it contains. If
`clean_immediately` is set, the table containing the Write Shard is then
destroyed because it is no longer useful and the process terminates on success.


Shards pool backends
--------------------

Winery support 2 types of pool to store shard files in which all the objects
accumulated in a read-write shard are packed:

- Ceph RBD (`rbd`): this is the original design; it uses directly Ceph RBD
  volumes to pack all content objects in, using the `swh-shard` file format.
  When a RBD volume has been created, or at starting time of a winery frontend
  node, RDB volumes are mounted on the winery frontend node to be usable as
  source to read objects from.

- Regular files (`directory`): in this backend, regular files are created in a
  directory (the `base_directory` configuration entry under the `shards_pool`
  section). In a production-like deployment, this directory will typically be
  made available on all winery front-end nodes via a shared storage solution
  like NFS or CephFS.


Distributed mode
----------------

`Winery` is usually deployed as a few separate components that synchronize each
other using the shared database (aka in a distributed mode):

* read-only instances provide access, in read-only mode, to both read-only
  shards, and shards that are currently being written to

* writer instances each hold one of the write tables locked, and write objects
  to them

* the shard packer `swh objstorage winery packer` handles the packing process
  asynchronously (outside of the `WineryWriter` process):

  * when a shard becomes `full`, it gets locked by the packer, and moved to the
    `packing` state

  * the shard file is created (when `create_images` is set) or waited for (if
    the management is delegated to the shard manager)

  * when the shard file is available, the shard gets packed

  * once the packing is done, the shard is moved to the `packed` state

  * if `clean_immediately` is set, the write shard is immediately removed and
    the shard moved to the `readonly` state

* the RBD shard manager `swh objstorage winery rbd` handles the management of
  RBD images:

  * all known `readonly` shards are mapped immediately

  * (if `manage_rw_images` is set) when a `standby` or `writing` shard appears,
    a new RBD image is provisioned in the Ceph cluster, and mapped read-write

  * when a shard packing completes (and a shard status becomes one of `packed`,
    `cleaning` or `readonly`), the image is mapped (or remapped) read-only.

  * every time a shard is mapped read-only on a given host, that fact is
    recorded in a database column

* the RW shard cleaner `swh objstorage winery rw-shard-cleaner` performs clean
  up of the `packed` read-write shards, as soon as they are recorded as mapped
  on enough (`--min-mapped-hosts`) hosts. They get locked in the `cleaning`
  state, the database cleanup is performed, then the shard gets moved in the
  final `readonly` state.


Configuration
-------------

`Winery` uses a structured configuration schema.

Here is a typical configuration for a RBD shards pool backend::

  objstorage:
    cls: winery

    # boolean (false (default): allow writes, true: only allow reads)
    readonly: false

    # Shards-related settings
    shards:
      # integer: threshold in bytes above which shards get packed. Can be
      # overflowed by the max allowed object size.
      max_size: 100_000_000_000

      # float: timeout in seconds after which idle read-write shards get
      # released by the winery writer process
      rw_idle_timeout: 300

    # Shared database settings
    database:
      # string: PostgreSQL connection string for the object index and read-write
      # shards
      db: winery

      # string: PostgreSQL application name for connections (unset by default)
      application_name: null

    # Shards pool settings
    shards_pool:
      ## Settings for the RBD shards pool
      type: rbd

      # Ceph pool name for RBD metadata (default: shards)
      pool_name: shards

      # Ceph pool name for RBD data (default: constructed as
      # `{pool_name}-data`). This is the pool where erasure-coding should be set,
      # if required.
      data_pool_name: null

      # Use sudo to perform image management (default: true. Can be set to false
      # if packer.create_images is false and the rbd image manager is deployed
      # as root)
      use_sudo: true

      # Options passed to `rbd image map` (default: empty string)
      map_options: ""

      # Image features unsupported by the RBD kernel module. E.g.
      # exclusive-lock, object-map and fast-diff, for Linux kernels older than 5.3
      image_features_unsupported: []

    # Packer-related settings
    packer:
      # Whether the winery writer should start packing shards immediately, or
      # defer to the standalone packer (default: true, the writer launches a
      # background packer process)
      pack_immediately: false

      # Whether the packer should create shards in the shard pool, or defer to
      # the pool manager (default: true, the packer creates images)
      create_images: false

      # Whether the packer should clean read-write shards from the database
      # immediately, or defer to the rw shard cleaner (default: true, the packer
      # cleans read-write shards immediately)
      clean_immediately: false

    # Optional throttler configuration, leave unset to disable throttling
    throttler:
      # string: PostgreSQL connection string for the throttler database. Can be
      # shared with (and defaults to) the main database set in the `database`
      # section. Must be read-write even for readonly instances.
      db: winery

      # integer: max read bytes per second
      max_read_bps: 100_000_000

      # integer: max write bytes per second
      max_write_bps: 100_000_000


Here is typical configuration for a directory shards pool backend::

  objstorage:
    cls: winery

    # boolean (false (default): allow writes, true: only allow reads)
    readonly: false

    # Shards-related settings
    shards:
      # integer: threshold in bytes above which shards get packed. Can be
      # overflowed by the max allowed object size.
      max_size: 100_000_000_000

      # float: timeout in seconds after which idle read-write shards get
      # released by the winery writer process
      rw_idle_timeout: 300

    # Shared database settings
    database:
      # string: PostgreSQL connection string for the object index and read-write
      # shards
      db: winery

      # string: PostgreSQL application name for connections (unset by default)
      application_name: null

    # Shards pool settings
    shards_pool:
      ## Settings for the directory shards pool
      # Shards are stored in `{base_directory}/{pool_name}`
      type: directory
      base_directory: /srv/winery/pool
      pool_name: shards

    # Packer-related settings
    packer:
      # Whether the winery writer should start packing shards immediately, or
      # defer to the standalone packer (default: true, the writer launches a
      # background packer process)
      pack_immediately: true

      # Whether the packer should create shards in the shard pool, or defer to
      # the pool manager (default: true, the packer creates images)
      create_images: true

      # Whether the packer should clean read-write shards from the database
      # immediately, or defer to the rw shard cleaner (default: true, the packer
      # cleans read-write shards immediately)
      clean_immediately: true

    # Optional throttler configuration, leave unset to disable throttling
    throttler:
      # string: PostgreSQL connection string for the throttler database. Can be
      # shared with (and defaults to) the main database set in the `database`
      # section. Must be read-write even for readonly instances.
      db: winery

      # integer: max read bytes per second
      max_read_bps: 100_000_000

      # integer: max write bytes per second
      max_write_bps: 100_000_000
