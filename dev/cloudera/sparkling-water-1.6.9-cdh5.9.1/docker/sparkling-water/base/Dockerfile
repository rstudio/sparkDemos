FROM ubuntu:trusty
MAINTAINER h2o.ai "h2o.ai" 
RUN \
   apt-get -q -y update && \
   apt-get -y install software-properties-common && \
   add-apt-repository ppa:webupd8team/java && \
   apt-get -q -y update && \
   ( echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections ) && \
   apt-get -y install oracle-java7-installer && \
   apt-get -y install oracle-java7-set-default && \
   apt-get clean
               
RUN \
    apt-get -y install \
      less \
      curl \
      sudo \
      unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
    rm -rf /var/cache/apt/*
                
RUN \
   curl -s http://mirrors.ocf.berkeley.edu/apache/spark/spark-1.6.0-cdh5.9.1/spark-1.6.0-cdh5.9.1-bin-hadoop2.6.tgz | tar -xz -C /opt && \
   ln -s /opt/spark-1.6.0-cdh5.9.1-bin-hadoop2.6 /opt/spark && \
   mkdir /opt/spark/work && \
   chmod 0777 /opt/spark/work
               
RUN \
   curl -s http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.6.0-cdh5.9/1/sparkling-water-1.6.0-cdh5.9.1.zip --output sw.zip && \
   unzip sw.zip -d /opt/ && \
   ln -s /opt/sparkling-water-1.6.0-cdh5.9.1 /opt/sparkling-water && \
   rm -f sw.zip
               
ENV SPARK_HOME /opt/spark
ENV SPARKLING_WATER_HOME /opt/sparkling-water
EXPOSE 4040 54321
WORKDIR /opt/sparkling-water
