#!/bin/bash

# spawn a MicroCeph instance, with enough OSDs for Winery's tests...
#
# ... however: when "rbd" is in a snap it cannot have enough permissions
# to do RBD mounts (cf. https://github.com/canonical/microceph/issues/175)
# so we rely on an additional stunt: use ceph/rbd binaries from the "ceph-common"
# debian package (cf. https://documentation.ubuntu.com/canonical-microceph/stable/snap/how-to/mount-block-device/)

sudo apt install -y ceph-common
sudo snap install microceph

# this creates conf & keyring by the way
sudo snap run microceph cluster bootstrap
# add storage: 6 Object Storage Daemons (OSDs) using 4GB files
sudo snap run microceph disk add loop,4G,6
sudo snap run microceph status

# pytest will use binaries from ceph-common, using microceph's config files
sudo ln -s /var/snap/microceph/current/conf/ceph.conf /etc/ceph/ceph.conf
sudo ln -s /var/snap/microceph/current/conf/ceph.keyring /etc/ceph/ceph.keyring

echo "waiting for the 6 OSDs to be visible from the client..."
while ! sudo ceph -s | grep -q 'osd: 6 osds'; do
    sleep 1
done

sudo ceph config set mon mon_allow_pool_delete true

echo "OK:"
sudo ceph -s

USE_CEPH=yes pytest $PYTEST_FLAGS swh/objstorage/tests/winery/test_objstorage_winery_rbd.py

# cleanup
sudo rm /etc/ceph/ceph.conf
sudo rm /etc/ceph/ceph.keyring
sudo snap remove microceph --purge
