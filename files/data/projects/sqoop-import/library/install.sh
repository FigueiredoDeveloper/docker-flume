#!/bin/bash

echo "Instala instaclient"
unzip dep-files/zip/instantclient-basic-linux.x64-11.2.0.4.0.zip -d /opt/

echo "Instala instaclient sdk"
unzip dep-files/zip/instantclient-sdk-linux.x64-11.2.0.4.0.zip -d /opt/

ln -s /opt/instantclient_11_2 /opt/instantclient
ln -s /opt/instantclient/libclntsh.so.11.1 /opt/instantclient/libclntsh.so

echo /opt/instaclient >/etc/ld.so.conf.d/oracle.conf
ldconfig

export ORACLE_HOME=/opt/instantclient
ORACLE_HOME=/opt/instantclient pip install cx_oracle

echo export LD_LIBRARY_PATH="/opt/instantclient:\$LD_LIBRARY_PATH" >>/etc/profile
