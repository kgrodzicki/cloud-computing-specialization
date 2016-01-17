FROM alpine:3.1
MAINTAINER Krzysztof Grodzicki <kgrodzicki@gmail.com>

RUN apk --update add openjdk7-jre
RUN apk --update add rsync
RUN apk --update add bash

ENV USER=hadoop
ENV HOME=/home/$USER
ENV HADOOP_CONFIG=$HOME/hadoop/etc/
RUN adduser -h "$HOME" -u 500 -D -s /bin/bash $USER

COPY .profile $HOME

RUN cd $HOME && \
    wget http://apache.mirror.gtcomm.net/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz && \
    tar zxvf hadoop-2.7.1.tar.gz && \
    mv hadoop-2.7.1 hadoop && \
    rm hadoop-2.7.1.tar.gz

COPY config/* $HADOOP_CONFIG
