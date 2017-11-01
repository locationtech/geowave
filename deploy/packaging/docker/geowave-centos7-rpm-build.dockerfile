FROM centos:centos7

RUN yum -y install asciidoc rpm-build unzip xmlto zip wget && \
    yum install -y ruby-devel gcc make rpm-build rubygems && \
    yum clean all

RUN gem install --no-ri --no-rdoc fpm
 
