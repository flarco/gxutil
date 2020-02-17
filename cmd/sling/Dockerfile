FROM golang:1.13

RUN apt update && apt install -y alien libaio1 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists /var/cache/apt

# Install Oracle Instant Client
# from https://apextips.blogspot.com/2019/09/installing-oracle-instant-client-on.html
RUN cd /tmp && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-devel-19.3.0.0.0-1.x86_64.rpm && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-sqlplus-19.3.0.0.0-1.x86_64.rpm && \
  alien -i oracle-instantclient19.3-*.rpm


RUN printf "\n\n# Oracle Client environment\n \
export LD_LIBRARY_PATH=/usr/lib/oracle/19.3/client64/lib/${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH} \
export ORACLE_HOME=/usr/lib/oracle/19.3/client64\n" | tee /etc/profile.d/oracle-env.sh > /dev/null

RUN echo " \
. /etc/profile.d/oracle-env.sh \
" >> /root/.bashrc

RUN cd /tmp && \
  wget https://github.com/flarco/gxutil/releases/download/sling-latest-Linux/sling-linux.gz && \
  gzip -d sling-linux.gz && \
  mv sling-linux /usr/local/go/bin/sling && \
  chmod 755 /usr/local/go/bin/sling

ENTRYPOINT sling