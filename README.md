Chaos
=====

What is it?
-----------

Chaos is a graph processing system for analytics on big graphs using small clusters. Chaos builds on the [X-Stream](https://github.com/epfl-labos/x-stream) single-machine graph processing system, but scales out to multiple machines. Chaos treats the aggregate storage of all machines as a single flat disk and uses work stealing to balance the load across nodes in the cluster. It exposes the familiar scatter-gather-apply programming model.

Installation
------------

Please see the file called [INSTALL.md](INSTALL.md).

Getting started
---------------

Like X-Stream, Chaos expects the input graphs in the binary edge list format. For details see the file called TYPES in the format directory. In addition to the edge list file, Chaos expects an ini file that specifies the graph type, the number of vertices and the number of edges.

When running on multiple machines, Chaos supports loading input graphs in parallel. To do so, each instance of Chaos must be provided with a part of the input. We recommend splitting the input in equal-sized parts and store one such part on each machine.

Provided with Chaos are two synthetic graph generators - a random graph generator and an RMAT graph generator. They also generate the needed ini files. For convenience, the generator can directly produce a part of a synthetic graph instead of the whole graph.

For example, to create the 3rd part of a scale-free graph with 1M vertices and 16M edges which is intended to run on 4 machines, run:
    
    $ rmat --name test --scale 20 --edges 16777216 --xscale_interval 4 --xscale_node 2

This creates a directed graph. To create an undirected graph instead, use `--symmetric`. Note that some algorithms require directed and some undirected graphs to work properly.

Directory format/tools contains a number of scripts to convert graphs from different formats into the format that Chaos expects. `split` is your friend when it comes to splitting the input for distribution to multiple machines (be careful to only split at edge boundaries!).

In order to provide context information to a Chaos instance (e.g., which other Chaos instances are running in the cluster, which ports to use for communication, etc.), `benchmark_driver` expected a file called `slipstore.ini` in the current directory. The structure of `slipstore.ini` is as follows:
 
    [machines]
    count=4						# number of machines
    me=2     					# machine id (starts at 0)
    name0=192.168.1.30			# hostname or IP address of first machine
    base_port0=5555				# base port of first machine
    iface0=eth1					# network interface of first machine
    name1=192.168.1.31			# hostname or IP address of second machine
    base_port1=5555				# base port of second machine
    iface1=eth1					# network interface of second machine
    ...							# repeat for all other machines in the cluster

To run any of the currently implemented algorithms in Chaos, you can use the provided example program called benchmark_driver. To see supported options run it with `-h`. For example, to run 10 iterations of pagerank on the test graph generated above, invoke benchmark_driver as:

    $ benchmark_driver -g test -b pagerank --pagerank::niters 10 -a -p 16 --physical_memory 268435456

This will run a Chaos instance with 16 worker threads and with 256MB of physical memory allotted to it. The -a flag tells Chaos to automatically tune the number of partitions and other system parameters.

When the run consists of multiple machines, you must run the above command for each instance! We recommend using `clush` or similar tools to make this easy.

Orchestrator
------------

Deploying, running and coordinating Chaos runs on many machines is arduous. We're planning to release our orchestration tool shortly, which should help with these tasks. Keep an eye out for the release.


Licensing
---------

Please see the file called LICENSE.

Contact
-------
- Laurent Bindschaedler <laurent.bindschaedler@epfl.ch>
- Jasmina Malicevic <jasmina.malicevic@epfl.ch>
- Amitabha Roy <amitabha.roy@gmail.com>
