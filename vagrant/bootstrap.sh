#!/usr/bin/env bash
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/mongodb.list
apt-get update
apt-get install -y mongodb-10gen=2.4.13
apt-get install -y python-dev
apt-get install -y python-pip
apt-get install -y vim
apt-get install -y libevent-dev
apt-get install -y python-virtualenv
cp /vagrant/vagrant/mongodb.conf /etc/mongodb.conf
cp /vagrant/vagrant/hosts /etc/hosts
mkdir /mnt/mongodb
chmod 777 /mnt/mongodb/
service mongodb restart
cd /vagrant && pip install MongoMultiMaster
HOSTNAME=$(hostname)
if [ $HOSTNAME == "mongo01r1" ]; then
  mongo -host localhost --eval "rs.initiate()"
  mongo -host localhost --eval "rs.add('mongo02r1')"
elif [ $HOSTNAME == "mongo01r2" ]; then
  mongo -host localhost --eval "rs.initiate()"
  mongo -host localhost --eval "rs.add('mongo02r2')"
elif [ $HOSTNAME == "mongo01r3" ]; then
  mongo -host localhost --eval "rs.initiate()"
  mongo -host localhost --eval "rs.add('mongo02r3')"
fi
mkdir /home/vagrant/mmm
chmod 777 /home/vagrant/mmm
cp vagrant/${HOSTNAME:7}_conf.yaml /home/vagrant/mmm/config.yaml
cp /vagrant/* /home/vagrant/mmm
rm -fr /home/vagrant/mmm/mmm
ln -s /vagrant/mmm /home/vagrant/mmm/mmm
ln -s /vagrant/vagrant/start /home/vagrant/mmm
ln -s /vagrant/vagrant/stop /home/vagrant/mmm
chmod 777 /home/vagrant/mmm/start
chmod 777 /home/vagrant/mmm/stop
