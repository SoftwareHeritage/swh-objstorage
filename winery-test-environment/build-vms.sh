#!/bin/bash

# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

set -e

: ${LIBVIRT_URI:=qemu:///system}
VIRSH="virsh --connect $LIBVIRT_URI"
VIRT_INSTALL="virt-install --connect $LIBVIRT_URI"

function ssh_key() {
    if ! test -f cluster_key; then
	ssh-keygen -f cluster_key -N '' -t rsa
    fi
    chmod 600 cluster_key
}

function stop() {
    local ids="$@"

    for id in $ids ; do
	$VIRSH destroy ceph$id >& /dev/null || true
	$VIRSH undefine ceph$id >& /dev/null || true
	rm -f ceph$id.qcow2
	rm -f disk$id*.img
    done
    $VIRSH net-destroy ceph >& /dev/null || true
    $VIRSH net-undefine ceph >& /dev/null || true
}

function start() {
    local ids="$@"

    ssh_key
    > ssh-config

    if ! test -f debian-11.qcow2 ; then
	sudo virt-builder debian-11 --output debian-11.qcow2 --size 10G --format qcow2 --install sudo --run-command 'dpkg-reconfigure --frontend=noninteractive openssh-server' --run-command 'useradd -s /bin/bash -m debian || true ; echo "debian ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-debian'  --ssh-inject debian:file:cluster_key.pub --edit '/etc/network/interfaces: s/ens2/enp1s0/'
    fi

    if ! $VIRSH net-list --name | grep ceph ; then
	cat > ceph-net.xml <<EOF
            <network>
              <name>ceph</name>
              <forward mode='nat'/>
              <bridge name='virbrceph' stp='on' delay='0'/>
              <ip address='10.11.12.1' netmask='255.255.255.0'>
                <dhcp>
                  <range start='10.11.12.100' end='10.11.12.200'/>
		  <host mac='52:54:00:00:00:01' name='ceph1' ip='10.11.12.211'/>
		  <host mac='52:54:00:00:00:02' name='ceph2' ip='10.11.12.212'/>
		  <host mac='52:54:00:00:00:03' name='ceph3' ip='10.11.12.213'/>
		  <host mac='52:54:00:00:00:04' name='ceph4' ip='10.11.12.214'/>
		  <host mac='52:54:00:00:00:05' name='ceph5' ip='10.11.12.215'/>
		  <host mac='52:54:00:00:00:06' name='ceph6' ip='10.11.12.216'/>
		  <host mac='52:54:00:00:00:07' name='ceph7' ip='10.11.12.217'/>
		  <host mac='52:54:00:00:00:08' name='ceph8' ip='10.11.12.218'/>
		  <host mac='52:54:00:00:00:09' name='ceph9' ip='10.11.12.219'/>
                </dhcp>
              </ip>
            </network>
EOF
	$VIRSH net-define ceph-net.xml
	$VIRSH net-start ceph
    fi


    for id in $ids ; do
	$VIRSH destroy ceph$id >& /dev/null || true
	$VIRSH undefine ceph$id >& /dev/null || true
	rm -f ceph$id.qcow2
	cp --sparse=always debian-11.qcow2 ceph$id.qcow2
	sudo virt-sysprep -a ceph$id.qcow2 --enable customize --hostname ceph$id
	$VIRT_INSTALL --network network=ceph,mac=52:54:00:00:00:0$id --boot hd --name ceph$id --memory 2048 --vcpus 1 --cpu host --disk path=$(pwd)/ceph$id.qcow2,bus=virtio,format=qcow2 --os-type=linux --os-variant=debian10 --graphics none --noautoconsole
	case $id in
	    1)
		;;
	    2)
		$VIRSH detach-device ceph$id ../rng.xml --live
		for drive in b c ; do
		    #
		    # Without the sleep it fails with:
		    #
		    # error: Failed to attach disk
		    # error: internal error: No more available PCI slots
		    #
		    sleep 10
		    rm -f disk$id$drive.img
		    qemu-img create -f raw disk$id$drive.img 20G
		    sudo chown libvirt-qemu disk$id$drive.img
		    $VIRSH attach-disk ceph$id --source $(pwd)/disk$id$drive.img --target vd$drive --persistent
		done
		;;
	    *)
		rm -f disk$id.img
		qemu-img create -f raw disk$id.img 20G
		sudo chown libvirt-qemu disk$id.img
		$VIRSH attach-disk ceph$id --source $(pwd)/disk$id.img --target vdb --persistent
		;;
	esac
	cat >> ssh-config <<EOF
Host ceph$id
    HostName 10.11.12.21$id
    Port 22
    User debian
    IdentityFile $(pwd)/cluster_key
    IdentityAgent none
    ForwardAgent yes
    TCPKeepAlive            yes
    Compression             no
    CheckHostIP no
    StrictHostKeyChecking no
EOF
    done
}

function restart() {
    local ids="$@"
    stop $ids
    start $ids
}

cd $(dirname $0)
mkdir -p context
ln -sf $(pwd)/libvirt.yml context/setup.yml
cd context

if test "$1" ; then
    "$@"
else
    restart 1 2 3 5 4 6 7 8
fi
