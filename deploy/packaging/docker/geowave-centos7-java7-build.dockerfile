FROM centos:centos7

RUN yum -y install asciidoc boost boost-devel gcc-c++ git glibc.i686 unzip which wget && \
    yum clean all

# Install repo containing python rpms
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm

# Install python, pip, and python development tools (Will install alongside system python as python3.6)
RUN yum -y install python36u python36u-pip python36u-devel

# Install asciidoctor
RUN yum -y install asciidoctor

RUN cd /tmp && wget --no-check-certificate --no-cookies \
    --header "Cookie: oraclelicense=accept-securebackup-cookie"  \
    http://download.oracle.com/otn-pub/java/jdk/7u79-b15/jdk-7u79-linux-x64.rpm -q && \
    rpm -Uvh /tmp/*.rpm && rm -fr /tmp/*.rpm && \
    wget http://archive.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.zip && \
    unzip apache-maven-3.6.0-bin.zip && \
    mv apache-maven-3.6.0/ /opt/maven && \
    ln -s /opt/maven/bin/mvn /usr/bin/mvn && \
    rm -rf apache-maven-3.6.0-bin.zip && \
    echo "export JAVA_HOME=/usr/java/latest" > /etc/profile.d/java_home.sh && cd ~
