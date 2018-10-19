FROM grycap/jenkins:ubuntu18.04-git as sbuilder
LABEL description="Batch input/output binary builder"
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
RUN pip3 install boto3 pyinstaller
RUN git clone https://github.com/alpegon/scar-batch-io
WORKDIR /scar-batch-io
RUN pyinstaller --onefile scarbatch_io.py

FROM ubuntu:latest
LABEL description="Batch input/output manager"
RUN addgroup --system scar && adduser --system --group scar
USER scar
WORKDIR /home/scar/
ENV SCAR_LOG_PATH=/home/scar/
COPY --from=sbuilder /scar-batch-io/dist/scarbatch_io /usr/bin/scar-batch-io