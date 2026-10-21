#!/bin/sh
# Make sure the docker-in-docker daemon is running.
#
# The docker-in-docker feature pins iptables to the legacy backend, but hosts
# whose kernels only provide nftables (e.g. Fedora) have no legacy `nat` table,
# so dockerd exits at startup ("can't initialize iptables table `nat'"). Switch
# to the nft backend and start it again.
set -e

docker info >/dev/null 2>&1 && exit 0

sudo update-alternatives --set iptables /usr/sbin/iptables-nft
sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-nft
sudo /usr/local/share/docker-init.sh true >/dev/null
