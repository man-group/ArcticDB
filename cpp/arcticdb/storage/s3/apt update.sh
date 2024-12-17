docker run --cap-add=NET_ADMIN --cap-add=NET_RAW --cap-add=SYS_PTRACE --security-opt seccomp=unconfined -v/tmp:/tmp/host -it ubuntu:22.04 /bin/bash

apt update
apt install -y software-properties-common
add-apt-repository ppa:deadsnakes/ppa -y
apt install -y python3.8 python3.8-venv python3-pip
python3.8 -m venv /root/pyenvs/venv38
cd /tmp/host

source /root/pyenvs/venv38/bin/activate
apt install -y iptables
apt install -y squid vim
pip install /tmp/host/arcticdb-4.1.0.dev0-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
pip install ipython

sh -c 'echo "
# Allow access from localhost
acl localnet src 127.0.0.1/32
http_access allow localnet

# Deny all other access
http_access deny all

# Set the port for the proxy server
http_port 3128
" >> /etc/squid/squid.conf'


service squid restart
iptables-nft -A OUTPUT -p tcp --dport 80 -m owner ! --uid-owner proxy -j REJECT
iptables-nft -A OUTPUT -p tcp --dport 443 -m owner ! --uid-owner proxy -j REJECT

export http_proxy="http://127.0.0.1:3128"
export https_proxy="http://127.0.0.1:3128"

unset http_proxy https_proxy

import arcticdb as adb
ac = adb.Arctic('s3://s3.eu-north-1.amazonaws.com:phoebus-test-bucket-1?aws_auth=sts&aws_profile=sts_test_profile&path_prefix=144_2024-12-18T10_36_20_161572')
ac.list_libraries()