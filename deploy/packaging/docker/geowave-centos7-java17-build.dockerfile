FROM centos:centos7

RUN yum -y install asciidoc boost boost-devel gcc-c++ git glibc.i686 unzip which wget && \
    yum clean all

# Install repo containing python rpms
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm

# Install python, pip, and python development tools (Will install alongside system python as python3.6)
RUN yum -y install python36u python36u-pip python36u-devel

# Install asciidoctor
RUN yum -y install asciidoctor

# Install OpenJDK 17 and Maven
RUN yum -y install java-17-openjdk java-17-openjdk-devel && \
    cd /tmp && \
    wget https://archive.apache.org/dist/maven/maven-3/3.9.4/binaries/apache-maven-3.9.4-bin.zip && \
    unzip apache-maven-3.9.4-bin.zip && \
    mv apache-maven-3.9.4/ /opt/maven && \
    ln -s /opt/maven/bin/mvn /usr/bin/mvn && \
    rm -rf apache-maven-3.9.4-bin.zip && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk" > /etc/profile.d/java_home.sh && cd ~
