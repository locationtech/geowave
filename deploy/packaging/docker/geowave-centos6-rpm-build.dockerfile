FROM centos:centos6

RUN yum -y install asciidoc rpm-build rpm2cpio tar unzip xmlto zip && \
	yum clean all
