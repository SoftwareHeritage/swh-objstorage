#!/bin/bash

# spawn a MicroCeph instance, with enough OSDs for Winery's tests...
#
# ... however: when "rbd" is in a snap it cannot have enough permissions
# to do RBD mounts (cf. https://github.com/canonical/microceph/issues/175)
# so we rely on additional stunts:
# 1. use ceph/rbd binaries from the "ceph-common" debian package
# (cf. https://documentation.ubuntu.com/canonical-microceph/stable/snap/how-to/mount-block-device/)
# 2. create udev rules for a dedicated unix group, so "pytest" can access RBD devices
# mounted via "sudo", because Winery contains a few open(pool.image_path(name), "rb"))

WGROUP="winery-writer"
echo "Checking $USER is in $WGROUP group so they can read-write RBD mounts..."
if ! getent group $WGROUP &> /dev/null; then
    sudo /usr/sbin/groupadd $WGROUP
fi
if ! id | grep $WGROUP &> /dev/null ; then
    sudo usermod -a -G $WGROUP $USER
    newgrp $WGROUP -c $0 # execution with the right group will be done in a sub-process
    exit 0
fi

RULES="/etc/udev/rules.d/55-rbd-winery.rules"
if [ ! -f "$RULES" ]; then
    echo "installing $RULES"
    sudo dd status=none of=$RULES <<END
KERNEL=="rbd[0-9]*", ENV{DEVTYPE}=="disk", ACTION=="add", ATTR{device/pool}=="*shards", ATTR{ro}=="0", GROUP="$WGROUP", MODE="0664"
KERNEL=="rbd[0-9]*", ENV{DEVTYPE}=="disk", ACTION=="add", ATTR{device/pool}=="*shards", ATTR{ro}!="0", GROUP="$WGROUP", MODE="0444"
KERNEL=="rbd[0-9]*", ENV{DEVTYPE}=="disk", ACTION=="add", ATTR{device/pool}=="*winery*", ATTR{ro}=="0", GROUP="$WGROUP", MODE="0664"
KERNEL=="rbd[0-9]*", ENV{DEVTYPE}=="disk", ACTION=="add", ATTR{device/pool}=="*winery*", ATTR{ro}!="0", GROUP="$WGROUP", MODE="0444"
END
    sudo udevadm control --reload-rules
    sudo udevadm trigger
fi

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
