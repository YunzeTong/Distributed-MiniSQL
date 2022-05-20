#!/bin/bash

# etcd
export TOKEN='my-etcd-token-1'
export CLUSTER_STATE='new'
export NAME_0='host0'
export NAME_1='host1'
export NAME_2='host2'
export NAME_3='host3'
export NAME_4='host4'
export HOST_0='172.18.0.2'
export HOST_1='172.18.0.3'
export HOST_2='172.18.0.4'
export HOST_3='172.18.0.5'
export HOST_4='172.18.0.6'

# https://unix.stackexchange.com/a/164167
set -- \
    "${NAME_0}=http://${HOST_0}:2380," \
    "${NAME_1}=http://${HOST_1}:2380," \
    "${NAME_2}=http://${HOST_2}:2380," \
    "${NAME_3}=http://${HOST_3}:2380," \
    "${NAME_4}=http://${HOST_4}:2380"
IFS=; export CLUSTER="$*"

# Distributed-MiniSQL
export DATA_DIR=sql/
export BRANCH=debug

git fetch --all
git reset --hard "origin/${BRANCH}"

rm -rf $DATA_DIR
mkdir $DATA_DIR