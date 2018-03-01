FROM centos:centos7

RUN yum -y install asciidoc rpm-build unzip xmlto zip wget \
    ruby-devel autoconf gcc make rpm-build rubygems automake \
    java-1.8.0-openjdk java-1.8.0-openjdk-devel libtool && \
    yum clean all && \
    cd /tmp && curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
    unzip awscli-bundle.zip && \
    ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
    cd ~

RUN gem install --no-ri --no-rdoc fpm
 
