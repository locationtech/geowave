FROM centos:centos7

RUN yum -y install epel-release && \
    yum -y install createrepo unzip zip wget && \
    yum clean all && \
    cd /tmp && curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
    unzip awscli-bundle.zip && \
    ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
    cd ~

RUN cd /tmp && \
    wget http://dl.fedoraproject.org/pub/epel/6/x86_64/hatools-2.14-1.1.el6.x86_64.rpm && \
    yum -y install hatools-2.14-1.1.el6.x86_64.rpm && \
    rm -rf hatools-2.14-1.1.el6.x86_64.rpm && \
    cd ~
