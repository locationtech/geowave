FROM centos:centos7

RUN yum -y install asciidoc rpm-build unzip xmlto zip && \
    yum clean all
