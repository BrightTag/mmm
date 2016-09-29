# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
  config.vm.provision :shell, inline: 'echo Hello'

  config.vm.define "mongo01r1" do |mongo01r1|
    mongo01r1.vm.box = "hashicorp/precise32"
    mongo01r1.vm.provision :shell, inline: "echo mongo01r1 > /etc/hostname"
    mongo01r1.vm.provision :shell, inline: "hostname mongo01r1"
    mongo01r1.vm.provision :shell, path: "vagrant/bootstrap.sh"
    mongo01r1.vm.network "private_network", ip: "10.200.1.201"
    mongo01r1.vm.network "forwarded_port", guest: 27017, host: 27011
  end

  config.vm.define "mongo02r1" do |mongo02r1|
    mongo02r1.vm.box = "hashicorp/precise32"
    mongo02r1.vm.provision :shell, path: "vagrant/bootstrap.sh"
    mongo02r1.vm.provision :shell, inline: "echo mongo02r1 > /etc/hostname"
    mongo02r1.vm.provision :shell, inline: "hostname mongo02r1"
    mongo02r1.vm.network "private_network", ip: "10.200.1.202"
    mongo02r1.vm.network "forwarded_port", guest: 27017, host: 27012
  end

  config.vm.define "mongo01r2" do |mongo01r2|
    mongo01r2.vm.box = "hashicorp/precise32"
    mongo01r2.vm.provision :shell, inline: "echo mongo01r2 > /etc/hostname"
    mongo01r2.vm.provision :shell, inline: "hostname mongo01r2"
    mongo01r2.vm.provision :shell, path: "vagrant/bootstrap.sh"
    mongo01r2.vm.network "private_network", ip: "10.200.1.203"
    mongo01r2.vm.network "forwarded_port", guest: 27017, host: 27013
  end

  config.vm.define "mongo02r2" do |mongo02r2|
    mongo02r2.vm.box = "hashicorp/precise32"
    mongo02r2.vm.provision :shell, inline: "echo mongo02r2 > /etc/hostname"
    mongo02r2.vm.provision :shell, inline: "hostname mongo02r2"
    mongo02r2.vm.provision :shell, path: "vagrant/bootstrap.sh"
    mongo02r2.vm.network "private_network", ip: "10.200.1.204"
    mongo02r2.vm.network "forwarded_port", guest: 27017, host: 27014
  end

  config.vm.define "mongo01r3" do |mongo01r3|
    mongo01r3.vm.box = "hashicorp/precise32"
    mongo01r3.vm.provision :shell, inline: "echo mongo01r3 > /etc/hostname"
    mongo01r3.vm.provision :shell, inline: "hostname mongo01r3"
    mongo01r3.vm.provision :shell, path: "vagrant/bootstrap.sh"
    mongo01r3.vm.network "private_network", ip: "10.200.1.205"
    mongo01r3.vm.network "forwarded_port", guest: 27017, host: 27015
  end

  config.vm.define "mongo02r3" do |mongo02r3|
    mongo02r3.vm.box = "hashicorp/precise32"
    mongo02r3.vm.provision :shell, inline: "echo mongo02r3 > /etc/hostname"
    mongo02r3.vm.provision :shell, inline: "hostname mongo02r3"
    mongo02r3.vm.provision :shell, path: "vagrant/bootstrap.sh"
    mongo02r3.vm.network "private_network", ip: "10.200.1.204"
    mongo02r3.vm.network "forwarded_port", guest: 27017, host: 27016
  end
end
