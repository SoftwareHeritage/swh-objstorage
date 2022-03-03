# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

set -e

function context() {
    local fed4fire=$1

    if ! test "$fed4fire" ; then
	return
    fi

    rm -fr ./context/fed4fire
    mkdir -p ./context/fed4fire
    cp $fed4fire ./context/fed4fire/fed4fire.zip
    local here=$(pwd)
    (
	cd ./context/fed4fire
	unzip fed4fire.zip
	sed -i \
	    -e 's|IdentityFile ./id_rsa$|IdentityFile '"${here}"'/context/cluster_key|' \
	    -e "s|-F ssh-config|-F ${here}/context/ssh-config|" \
	    ssh-config
	cp ssh-config ..
	mv id_rsa ../cluster_key
	mv id_rsa.pub ../cluster_key.pub
    )
}

ln -sf $(pwd)/grid5000.yml context/setup.yml

context "$@"
