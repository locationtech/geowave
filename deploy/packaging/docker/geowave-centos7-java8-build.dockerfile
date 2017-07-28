FROM centos:centos7

RUN yum -y install asciidoc boost boost-devel gcc-c++ git glibc.i686 unzip which wget && \
    yum clean all

RUN cd /tmp && wget --no-check-certificate --no-cookies \
    --header "Cookie: oraclelicense=accept-securebackup-cookie" \
    http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.rpm && \
    rpm -Uvh /tmp/*.rpm && rm -fr /tmp/*.rpm && \
    wget http://archive.apache.org/dist/maven/binaries/apache-maven-3.2.2-bin.zip && \
    unzip apache-maven-3.2.2-bin.zip && \
    mv apache-maven-3.2.2/ /opt/maven && \
    ln -s /opt/maven/bin/mvn /usr/bin/mvn && \
    rm -rf apache-maven-3.2.2-bin.zip && \
    echo "export JAVA_HOME=/usr/java/latest" > /etc/profile.d/java_home.sh && cd ~
