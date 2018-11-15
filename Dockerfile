FROM grycap/jenkins:ubuntu18.04-git as sbuilder
LABEL description="Batch input/output binary builder"
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
RUN pip3 install boto3 urllib3 pyinstaller
RUN mkdir /scar-batch-io
WORKDIR /scar-batch-io
COPY . .
RUN pyinstaller --onefile --hidden-import=urllib3 scarbatch_io.py

FROM ubuntu:latest
LABEL description="Batch input/output manager"
RUN apt-get update && apt-get install -y \
    ca-certificates \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV SCAR_LOG_PATH=/var/log/
COPY --from=sbuilder /scar-batch-io/dist/scarbatch_io /usr/bin/scar-batch-io