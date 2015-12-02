#!/usr/bin/env bash
#
# Installing additional components on an EMR node depends on several config files
# controlled by the EMR framework which may affect the is_master and configure_zookeeper
# functions at some point in the future. I've grouped each unit of work into a function 
# with a descriptive name to help with understanding and maintainability
#

# You can change these but there is probably no need
ACCUMULO_INSTANCE=accumulo
ACCUMULO_HOME="${INSTALL_DIR}/accumulo"
ZK_IPADDR=
INTIAL_POLLING_INTERVAL=15 # This gets doubled for each attempt up to max_attempts
HDFS_USER=hdfs

# Parses a configuration file put in place by EMR to determine the role of this node
is_master() {
  if [ $(jq '.isMaster' /mnt/var/lib/info/instance.json) = 'true' ]; then
    return 0
  else
    return 1
  fi
}

# Avoid race conditions and actually poll for availability of component dependencies
# Credit: http://stackoverflow.com/questions/8350942/how-to-re-run-the-curl-command-automatically-when-the-error-occurs/8351489#8351489
with_backoff() {
  local max_attempts=${ATTEMPTS-5}
  local timeout=${INTIAL_POLLING_INTERVAL-1}
  local attempt=0
  local exitCode=0

  while (( $attempt < $max_attempts ))
  do
    set +e
    "$@"
    exitCode=$?
    set -e

    if [[ $exitCode == 0 ]]
    then
      break
    fi

    echo "Retrying $@ in $timeout.." 1>&2
    sleep $timeout
    attempt=$(( attempt + 1 ))
    timeout=$(( timeout * 2 ))
  done

  if [[ $exitCode != 0 ]]
  then
    echo "Fail: $@ failed to complete after $max_attempts attempts" 1>&2
  fi

  return $exitCode
}

# Using zookeeper packaged by Apache BigTop for ease of installation
configure_zookeeper() {
	if is_master ; then
		sudo sh -c "curl http://www.apache.org/dist/bigtop/bigtop-1.0.0/repos/centos6/bigtop.repo > /etc/yum.repos.d/bigtop.repo"
		sudo yum -y install zookeeper-server
		sudo service zookeeper-server start # EMR uses Amazon Linux which uses Upstart
		# Zookeeper installed on this node, record internal ip from instance metadata
		ZK_IPADDR=$(curl http://169.254.169.254/latest/meta-data/local-ipv4)
	else
		# Zookeeper intalled on master node, parse config file to find EMR master node
		ZK_IPADDR=$(xmllint --xpath "//property[name='yarn.resourcemanager.hostname']/value/text()"  /etc/hadoop/conf/yarn-site.xml)
	fi
}

is_hdfs_available() {
	hadoop fs -ls /
	return $?
}

wait_until_hdfs_is_available() {
	with_backoff is_hdfs_available
	if [ $? != 0 ]; then
		echo "HDFS not available before timeout. Exiting ..."
		exit 1
	fi
}

# Settings recommended for Accumulo
os_tweaks() {
	echo -e "net.ipv6.conf.all.disable_ipv6 = 1" | sudo tee --append /etc/sysctl.conf
	echo -e "net.ipv6.conf.default.disable_ipv6 = 1" | sudo tee --append /etc/sysctl.conf
	echo -e "net.ipv6.conf.lo.disable_ipv6 = 1" | sudo tee --append /etc/sysctl.conf
	echo -e "vm.swappiness = 0" | sudo tee --append /etc/sysctl.conf
	sudo sysctl -w vm.swappiness=0
	echo -e "" | sudo tee --append /etc/security/limits.conf
	echo -e "*\t\tsoft\tnofile\t65536" | sudo tee --append /etc/security/limits.conf
	echo -e "*\t\thard\tnofile\t65536" | sudo tee --append /etc/security/limits.conf
}

initialize_volumes() {
    local VOLS=$(lsblk | awk '{ print $1 }' | grep -v NAME)

    for v in $VOLS; do
        sudo fio --filename=/dev/${v} --rw=randread --bs=128k --iodepth=32 --ioengine=libaio --direct=1 --name=volume-init >> /tmp/emr-vol-init.log
    done
}

create_accumulo_user() {
 	id $USER
	if [ $? != 0 ]; then
		sudo adduser $USER
		sudo sh -c "echo '$USERPW' | passwd $USER --stdin"
	fi
}

install_accumulo() {
	wait_until_hdfs_is_available
	ARCHIVE_FILE="accumulo-${ACCUMULO_VERSION}-bin.tar.gz"
	LOCAL_ARCHIVE="${INSTALL_DIR}/${ARCHIVE_FILE}"
	sudo sh -c "curl 'http://apache.mirrors.tds.net/accumulo/${ACCUMULO_VERSION}/${ARCHIVE_FILE}' > $LOCAL_ARCHIVE"
	sudo sh -c "tar xzf $LOCAL_ARCHIVE -C $INSTALL_DIR"
	sudo rm -f $LOCAL_ARCHIVE
	sudo ln -s "${INSTALL_DIR}/accumulo-${ACCUMULO_VERSION}" "${INSTALL_DIR}/accumulo"
	sudo chown -R accumulo:accumulo "${INSTALL_DIR}/accumulo-${ACCUMULO_VERSION}"
	sudo sh -c "echo 'export PATH=$PATH:${INSTALL_DIR}/accumulo/bin' > /etc/profile.d/accumulo.sh"
}

configure_accumulo() {
	sudo cp $INSTALL_DIR/accumulo/conf/examples/${ACCUMULO_TSERVER_OPTS}/native-standalone/* $INSTALL_DIR/accumulo/conf/
	sudo sed -i "s/<value>localhost:2181<\/value>/<value>${ZK_IPADDR}:2181<\/value>/" $INSTALL_DIR/accumulo/conf/accumulo-site.xml
	sudo sed -i '/HDP 2.0 requirements/d' $INSTALL_DIR/accumulo/conf/accumulo-site.xml
	sudo sed -i "s/\${LOG4J_JAR}/\${LOG4J_JAR}:\/usr\/lib\/hadoop\/lib\/*:\/usr\/lib\/hadoop\/client\/*/" $INSTALL_DIR/accumulo/bin/accumulo

	# Crazy escaping to get this shell to fill in values but root to write out the file
	ENV_FILE="export ACCUMULO_HOME=$INSTALL_DIR/accumulo; export HADOOP_HOME=/usr/lib/hadoop; export ACCUMULO_LOG_DIR=$INSTALL_DIR/accumulo/logs; export JAVA_HOME=/usr/lib/jvm/java; export ZOOKEEPER_HOME=/usr/lib/zookeeper; export HADOOP_PREFIX=/usr/lib/hadoop; export HADOOP_CONF_DIR=/etc/hadoop/conf"
	echo $ENV_FILE > /tmp/acc_env
	sudo sh -c "cat /tmp/acc_env > $INSTALL_DIR/accumulo/conf/accumulo-env.sh"
	sudo chown -R $USER:$USER $INSTALL_DIR/accumulo
	source $INSTALL_DIR/accumulo/conf/accumulo-env.sh
	sudo -u $USER $INSTALL_DIR/accumulo/bin/build_native_library.sh

	if is_master ; then
	    sudo -u $HDFS_USER hadoop fs -chmod 777 /user # This is more for Spark than Accumulo but put here for expediency
		sudo -u $HDFS_USER hadoop fs -mkdir /accumulo
		sudo -u $HDFS_USER hadoop fs -chown accumulo:accumulo /accumulo
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/monitor"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/gc"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/tracers"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/masters"
		sudo sh -c "echo > $INSTALL_DIR/accumulo/conf/slaves"
		sudo -u $USER $INSTALL_DIR/accumulo/bin/accumulo init --clear-instance-name --instance-name $ACCUMULO_INSTANCE --password $USERPW
	else
		sudo sh -c "echo $ZK_IPADDR > $INSTALL_DIR/accumulo/conf/monitor"
		sudo sh -c "echo $ZK_IPADDR > $INSTALL_DIR/accumulo/conf/gc"
		sudo sh -c "echo $ZK_IPADDR > $INSTALL_DIR/accumulo/conf/tracers"
		sudo sh -c "echo $ZK_IPADDR > $INSTALL_DIR/accumulo/conf/masters"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/slaves"
	fi

	# EMR starts worker instances first so there will be timing issues
	# Test to ensure it's safe to continue before attempting to start things up
	if is_master ; then
		with_backoff is_accumulo_initialized
	else
		with_backoff is_accumulo_available
	fi

	sudo -u $USER $INSTALL_DIR/accumulo/bin/start-here.sh
}

is_accumulo_initialized() {
	hadoop fs -ls /accumulo/instance_id
	return $?
}

is_accumulo_available() {
	$INSTALL_DIR/accumulo/bin/accumulo info
	return $?
}

install_image_libs() {
	pushd /etc/alternatives/java_sdk
	sudo curl -O $JAI_URL
	sudo curl -O $IMAGEIO_URL
	sudo chmod +x *.bin
	# Magic spells to unzip and install with auto-confirm of terms and bypassing unzip error with export
	sudo ./jai-*.bin >/dev/null < <(echo y) >/dev/null < <(echo y)
	sudo bash -c "export _POSIX2_VERSION=199209; ./jai_*.bin >/dev/null < <(echo y) >/dev/null < <(echo y)"
	popd
}

install_geowave() {
	# Install the repo config file
	sudo rpm -Uvh http://s3.amazonaws.com/geowave-rpms/dev/noarch/$GEOWAVE_REPO_RPM

	# EMR has a tar bundle installed puppet in /home/ec2-user 
	# So as not to install incompatible puppet from the dependencies of geowave-puppet
	# I'm doing this convoluted workaround to download and then install with no dep resolution
	sudo yumdownloader --enablerepo geowave-dev --destdir /tmp geowave-puppet
	sudo rpm -Uvh --force --nodeps /tmp/geowave-puppet.*.noarch.rpm 

cat << EOF > /tmp/geowave.pp
class { 'geowave::repo': 
  repo_base_url => 'http://s3.amazonaws.com/geowave-rpms/dev/noarch/',
  repo_enabled  => 1,
} ->
class { 'geowave':
  geowave_version       => '${GEOWAVE_VERSION}',
  hadoop_vendor_version => 'apache',
  install_accumulo      => true,
  install_app           => true,
  install_app_server    => true,
  http_port             => '${GEOSERVER_PORT}',
}

file { '/etc/geowave/geowave.config':
	ensure  => file,
	owner   => 'geowave',
	group   => 'geowave',
	mode    => 644,
	content => 'export JAVA_OPTS="${GEOSERVER_MEMORY}"',
	require => Package['geowave-core'],
	notify  => Service['geowave'],
}
EOF

	sudo sh -c "puppet apply /tmp/geowave.pp"
	return 0
}
