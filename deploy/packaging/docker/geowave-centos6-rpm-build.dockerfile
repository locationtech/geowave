FROM centos:centos6

RUN yum -y install asciidoc rpm-build tar unzip xmlto zip && \
	yum clean all
