FROM ubuntu
RUN apt-get update
RUN apt-get -y install python
RUN apt-get install -y python-pip
RUN pip install boto3 --user
COPY supervisor /
COPY script /usr/local/bin
RUN ["chmod", "+x", "/usr/local/bin/script.sh"]
#ENTRYPOINT [ "sh", "/script.sh" ]