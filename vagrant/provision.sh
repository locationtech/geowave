# Install dependencies

# Setup JDK
echo "JDK: Downloading..."
wget -q --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/7u75-b13/jdk-7u75-linux-x64.rpm -O jdk-7u75-linux-x64.rpm
echo "JDK: Installing..."
sudo yum -y --nogpgcheck localinstall jdk-7u75-linux-x64.rpm

# Setup Maven
echo "Maven: Downloading..."
wget -q http://apache.arvixe.com/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
echo "Maven: Installing..."
sudo tar -xzf apache-maven-3.2.5-bin.tar.gz -C /usr/local
sudo ln -s /usr/local/apache-maven-3.2.5 /usr/local/maven
sudo printf 'export M2_HOME=/usr/local/maven\nexport M2=$M2_HOME/bin\nexport PATH=$M2:$PATH' > /etc/profile.d/maven.sh
source /etc/profile

# Setup Git
echo "Git: Installing..."
sudo yum -y install git

# Setup GeoWave
echo "GeoWave: Cloning..."
git clone https://github.com/ngageoint/geowave.git
cd geowave
git checkout geowave-vagrant
echo "GeoWave: Building..."
mvn clean package -pl geowave-deploy -am -P geowave-singlejar -DskipITs=true -DskipTests=true

# Install GeoWave service
echo "GeoWave: Installing Service..."
sudo cp /home/vagrant/geowave/vagrant/geowave.sh /etc/init.d/geowave
sudo chkconfig --add geowave
sudo service geowave start
