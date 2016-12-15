FROM centos:centos6

RUN yum -y install epel-release && \
    yum -y install hatools createrepo rpm2cpio tar unzip zip && \
	yum clean all && \
	cd /tmp && curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
	unzip awscli-bundle.zip && \
	./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && cd ~
	