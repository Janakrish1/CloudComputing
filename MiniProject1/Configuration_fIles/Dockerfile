FROM ubuntu:18.04

USER root
ENV hadoop_path /opt/hadoop
ENV java_path /usr/lib/jvm/java-8-openjdk-amd64
ENV HDFS_DATANODE_USER=root \
    HDFS_NAMENODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    HADOOP_SECURE_DN_USER=hdfs \
    YARN_RESOURCEMANAGER_USER=root \
    HADOOP_SECURE_DN_USER=yarn \
    YARN_NODEMANAGER_USER=root
    

RUN \
   apt-get update && apt-get install -y \
   ssh \
   rsync \
   vim \
   openjdk-8-jdk

RUN \
   wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz \
    && \
    tar -xzf hadoop-3.4.0.tar.gz && \
    mv hadoop-3.4.0 $hadoop_path && \
    echo "export JAVA_HOME=$java_path" >> $hadoop_path/etc/hadoop/hadoop-env.sh && \
    echo "PATH=$PATH:$hadoop_path/bin" >> ~/.bashrc
RUN /etc/init.d/ssh start

ADD *.xml $hadoop_path/etc/hadoop/
ADD bootstrap.sh bootstrap.sh

EXPOSE 8088 50070 50075 50030 50060
cmd bash bootstrap.sh




#docker build .
#sudo docker images
#sudo docker run -it -d name /bin/bash
#
#sudo docker ps
#sudo docker exec -it name
