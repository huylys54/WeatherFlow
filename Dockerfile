FROM spark:3.5.5-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    ln -s /usr/bin/python3 /usr/bin/python; \
    rm -rf /var/lib/apt/lists/*;
# Set the shell to bash to support source
SHELL ["/bin/bash", "-c"]

# Add aliases to .bashrc and source it
RUN echo 'alias python="/usr/bin/python3"' >> /root/.bashrc && \
    echo 'alias pip="/usr/bin/pip3"' >> /root/.bashrc && \
    source /root/.bashrc

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN  pip install --no-cache-dir -r requirements.txt

COPY scripts/extract_weather.py .
COPY scripts/transform_weather.py .
COPY scripts/load_weather.py .
COPY .env .