FROM ubuntu:20.04

EXPOSE 8000

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install \
        fastapi==0.88.0 \
        minio==7.1.12 \
        python-dotenv==0.21.0 \
        requests==2.28.1 \
        uvicorn==0.20.0  \
        xattr==0.10.1

COPY amc2_api-*.whl /src/

RUN pip3 install /src/*.whl

CMD ["/usr/local/bin/uvicorn", "--host", "0.0.0.0", "--port", "8000", "amc2_api.api:app"]