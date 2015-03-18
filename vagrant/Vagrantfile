# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "chef/centos-6.5"
  config.vm.hostname = "geowave-vagrant"
  config.vm.host_name = "geowave-vagrant"
  
  config.ssh.forward_agent = true

  # Port forwarding for Accumulo Monitor
  config.vm.network :forwarded_port, guest: 50095, host:50095

  # Port forwarding for Zookeeper 
  config.vm.network :forwarded_port, guest: 2181, host:2181

  # Setup a synced folder in the current directory
  config.vm.synced_folder "./geowave", "/home/vagrant/geowave", create: true

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", 4096]
    vb.customize ["modifyvm", :id, "--cpus", 2]
    vb.customize ["modifyvm", :id, "--ioapic", "on"]
    vb.name = "geowave-vagrant"
    vb.gui = true
  end

  config.vm.provision :shell, :path => "provision.sh"
end
