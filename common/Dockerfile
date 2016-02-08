FROM mousera/base-image:4842


USER root
RUN mkdir -p /app/
# Ghetto workaround for AUFS bug
COPY ./common /tmp/common
RUN mv /tmp/common /app/

# The following lines were moved from the analytics container
RUN apt-get clean
RUN apt-get update
RUN apt-get install -f -y python-pip python-sklearn python-numpy python-gflags rsyslog

RUN cp /app/common/syslog/loggly.conf /etc/rsyslog.d/22-loggly.conf
RUN cp /app/common/syslog/rsyslog /etc/logrotate.d/rsyslog

RUN pip install -r /app/common/requirements.txt --no-cache-dir
