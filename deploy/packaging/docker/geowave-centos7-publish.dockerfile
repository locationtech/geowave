FROM centos:centos7
ARG third_party_deps_path

RUN yum -y install epel-release && \
    yum -y install createrepo unzip zip wget && \
    yum clean all && \
    cd /tmp && curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
    unzip awscli-bundle.zip && \
    ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
    cd ~

RUN cd /tmp && \
    wget ${third_party_deps_path}/hatools/hatools-2.14-1.1.el6.x86_64.rpm && \
    yum -y install hatools-2.14-1.1.el6.x86_64.rpm && \
    rm -rf hatools-2.14-1.1.el6.x86_64.rpm && \
    cd ~
