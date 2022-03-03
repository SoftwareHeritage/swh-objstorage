.. _swh-objstorage-winery:

Winery backend
==============

The Winery backend implements the `Ceph based object storage architecture <https://wiki.softwareheritage.org/wiki/A_practical_approach_to_efficiently_store_100_billions_small_objects_in_Ceph>`__.

IO Throttling
--------------

Ceph (Pacific) implements IO QoS in librbd but it is only effective within a single process, not cluster wide. The preliminary benchmarks showed that accumulated read and write throughput must be throttled client side to prevent performance degradation (slower throughput and increased latency).

Table are created in a PostgreSQL database dedicated to throttling, so independent processes performing I/O against the Ceph cluster can synchronize with each other and control their accumulated throughput for reads and writes. Workers creates a row in the read and write tables and update them every minute with their current read and write throughput, in bytes per second. They also query all rows to figure out the current accumulated bandwidth.

If the current accumulated bandwidth is above the maximum desired speed for N active workers, the process will reduce its throughput to use a maximum of 1/N of the maximum desired speed. For instance, if the current accumulated usage is above 100MB/s and there are 10 workers, the process will reduce its own speed to 10MB/s. After the 10 workers independently do the same, each of them will share 1/10 of the bandwidth.

Implementation notes
--------------------

The `sharedstorage.py` file contains the global index implementation that associates every object id to the shard it contains. A list of shard (either writable or readonly) is stored in a table, with a numeric id to save space. The name of the shard is used to create a database (for write shards) or a RBD image (for read shards).

The `roshard.py` file contain the lookup function for a read shard and is a thin layer on top of swh-perfect hash.

The `rwshard.py` file contains the logic to read, write and enumerate the objects of a write shard using SQL statements on the database dedicated to it.

The `obstorage.py` file contains the backend implementation in the `WineryObjStorage` class. It is a thin layer that delegates writes to a `WineryWriter` instance and reads to a `WineryReader` instance. Although they are currently tightly coupled, they are likely to eventually run in different workers if performance and security requires it.

A `WineryReader` must be able to read an object from both Read Shards and Write Shards. It will first determine the kind of shard the object belongs to by looking it up in the global index. If it is a Read Shard, it will lookup the object using the `ROShard` class from `roshard.py`, ultimately using a Ceph RBD image.  If it is a Write Shard, it will lookup the object using the `RWShard` class from `rwshard.py`, ultimately using a PostgreSQL database.

All `WineryWriter` operations are idempotent so they can be resumed in case they fail. When a `WineryWriter` is instantiated, it will either:

* Find a Write Shard (i.e. a database) that is not locked by another instance by looking up the list of shards or,
* Create a new Write Shard by creating a new database

and it will lock the Write Shard and own it so no other instance tries to write to it. A PostgreSQL session lock is used to lock the shard so that it is released when the `WineryWrite` process dies unexpectedly and another process can pick it up.

When a new object is added to the Write Shard, a new row is added to the global index to record that it is owned by this Write Shard and is in flight. Such an object cannot be read because it is not yet complete. If a request to write the same object is sent to another `WineryWriter` instance, it will fail to add it to the global index because it already exists. Since the object is in flight, the `WineryWriter` will check if the shard associated to the object is:

* its name, which means it owns the object and must resume writing the object
* not its name, which means another `WineryWriter` owns it and nothing needs to be done

After the content of the object is successfully added to the Write Shard, the state of the record in the global index is modified to no longer be in flight. The client is notified that the operation was successful and the object can be read from the Write Shard from that point on.

When the size of the database associated with a Write Shard exceeds a threshold, it is set to be in the `packing` state. All objects it contains can be read from it by any `WineryReader` but no new object will be added to it. A process is spawned and is tasked to transform it into a Read Shard using the `Packer` class. Should it fail for any reason, a cron job will restart it when it finds Write Shards that are both in the `packing` state and not locked by any process. Packing is done by enumerating all the records from the Write Shard database and writing them into a Read Shard by the same name. Incomplete Read Shards will never be used by `WineryReader` because the global index will direct it to use the Write Shard instead. Once the packing completes, the state of the shard is modified to be readonly and from that point on the `WineryReader` will only use the Read Shard to find the objects it contains. The database containing the Write Shard is then destroyed because it is no longer useful and the process terminates on success.

Benchmarks
----------

Follow the instructions at winery-test-environment/README.md
