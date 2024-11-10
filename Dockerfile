FROM bitnami/spark:latest

USER root

# Update and install Python and required tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/bitnami/java

WORKDIR /app
COPY . /app

# Make Python 3 the default Python
RUN ln -sf /usr/bin/python3 /usr/bin/python
