## Step #1: Configure a Docker build host

A host to run the GeoWave build containers needs just Docker, Git and the Unzip commands available. Tested Docker
configurations are shown below but any OS capable of running Docker containers should work.

### Redhat7/CentOS7 Docker Build Host

```
sudo yum -y install docker git unzip
sudo systemctl start docker
sudo systemctl enable docker
```

### Ubuntu 14.04 Build Host
```
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
sudo sh -c "echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
sudo apt-get update
sudo apt-get -y install lxc-docker git unzip
```

### Docker Test

Before continuing test that Docker is available with the `sudo docker info` command

## Step #2: GeoWave Source Code

From the docker build host we're going to clone the GeoWave repo and then by using volume mounts 
we'll allow the various containers to build and/or package the code without the need to then copy 
the finished artifacts back out of the container.

```
git clone --depth 1 https://github.com/locationtech/geowave.git
```

## Step #3: Create Docker Images for Building

We'll eventually publish these images, until then you'll have to build them locally

```
pushd geowave/deploy/packaging/docker
sudo docker build -t locationtech/geowave-centos7-java8-build -f geowave-centos7-java8-build.dockerfile .   
sudo docker build -t locationtech/geowave-centos7-rpm-build -f geowave-centos7-rpm-build.dockerfile .
popd
```

## Step #4: Build GeoWave Artifacts and RPMs

The docker-build-rpms script will coordinate a series of container builds resulting in finished jar and rpm artifacts
built for each of the desired build configurations (ex: cdh5, hortonworks or apache).

```
export WORKSPACE="$(pwd)/geowave"
export SKIP_TESTS="-Dfindbugs.skip=true -Dformatter.skip=true -DskipITs=true -DskipTests=true" # (Optional)
sudo chown -R $(whoami) geowave/deploy/packaging
geowave/deploy/packaging/docker/docker-build-rpms.sh
```

After the docker-build-rpms.sh command has finished the rpms can be found in the 
`geowave/deploy/packaging/rpm/centos/7/RPMS/noarch/` directory adjusting the version of the OS as needed.
