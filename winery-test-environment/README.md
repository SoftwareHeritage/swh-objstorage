This purpose of these instructions is to run `tox -e py3` in an
environment that has access to a ceph cluster. It enables tests that
would be otherwise be skipped and increases code coverage.

The environment is composed of eight machines named ceph1 to ceph8.

# Installation

* pip install -r requirements.txt
* ansible-galaxy install geerlingguy.docker

# Create the machines

## libvirt

* ensure virsh is available
* ./build-vms.sh

If the internet cnx is slow it may take a while before the OSD show up
because they require downloading large docker images.

## fed4fire

### Create a base rspec specification.

* /opt/jFed/jFed-Experimenter
* In the General Tab
* Create an experiment (New)
* Add one Physical Node by dragging it
* Right click on the node and choose "Configure Node"
* Select testbed: Grid 5000
* Node => Specific hardware type: dahu-grenoble
* Disk image => Bullseye base
* Save under sample.rspec
* Manually edit to duplicate the nodes

### Run the experiment.

* /opt/jFed/jFed-Experimenter
* In the General Tab
* Open Local and load winery-test-environment/fed4fire.rspec
* Edit ceph1 node to check if the Specific hardware type is dahu-grenoble
* Click on Topology Viewer
* Run
* Give a unique name to the experiment
* Start experiment
* Once the provisionning is complete (Testing connectivity to resources on Grid5000) click "Export As"
* Choose "Export Configuration Management Settings"
* Save under /tmp/test.zip
* fed4fire.sh test.zip

# Install the machines

* ansible-playbook -i inventory context/setup.yml ceph.yml bootstrap.yml osd.yml tests.yml

# Run the tests

It copies the content of the repository and "ssh ceph1 tox -e py3"

* tox -e winery

# Login into a machine

For each host found in context/ssh-config

* ssh -i context/cluster_key -F context/ssh-config ceph1

# Run the benchmarks

The `tox -e winery` command is used to run the benchmarks with the desired parameters. Upon completion the raw data can be found in the `winery-test-environment/context/stats` directory and is displayed on the standard output as well as rendered in a graph, if a display is available (see the `winery-test-environment/render-stats.py` for the details).

### Example

* tox -e winery -- -s --log-cli-level=INFO -vvv -k test_winery_bench_real --winery-bench-duration 30  --winery-shard-max-size $((10 * 1024 * 1024)) --winery-bench-ro-worker-max-request 2000

### Get all benchmark flags

Run the following command and look for flags that start with `--winery-bench-`

* tox -e winery -- --help

# Destroy

## libvirt

* ./build-vms.sh stop $(seq 1 8)

## fed4fire

It will expire on its own
