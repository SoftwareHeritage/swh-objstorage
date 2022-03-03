# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

set -ex

DIR=winery-test-environment
SSH="ssh -i ${DIR}/context/cluster_key -F ${DIR}/context/ssh-config"

function sanity_check() {
    if ! test -f ${DIR}/context/cluster_key ; then
	echo "${DIR}/context/cluster_key does not exist"
	echo "check ${DIR}/README.md for instructions."
	return 1
    fi
}

function copy_to() {
    RSYNC_RSH="$SSH" rsync -av --exclude=.mypy_cache --exclude=.coverage --exclude=.eggs --exclude=swh.objstorage.egg-info --exclude=winery-test-environment/context --exclude=.tox --exclude='*~' --exclude=__pycache__ --exclude='*.py[co]' $(git rev-parse --show-toplevel)/ debian@ceph1:/home/debian/swh-objstorage/
}

function copy_from() {
    RSYNC_RSH="$SSH" rsync -av --delete debian@ceph1:/tmp/winery/ ${DIR}/context/stats/
}

function render() {
    python ${DIR}/render-stats.py ${DIR}/context/stats/
}

function run() {
    sanity_check || return 1

    copy_to || return 1

    $SSH -t debian@ceph1 bash -c "'cd swh-objstorage ; ../venv/bin/tox -e py3 -- -k test_winery $*'" || return 1

    copy_from || return 1

    render || return 1
}

run "$@"
