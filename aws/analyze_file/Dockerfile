FROM public.ecr.aws/lambda/python:3.12

# Install build tools and required libs
RUN dnf install -y \
    gcc-c++ autoconf automake libtool make pkgconfig \
    libjpeg-devel libpng-devel libtiff-devel zlib-devel \
    mesa-libGL \
    openssl wget tar gzip xz \ \
 && dnf clean all \

# Build Leptonica
ENV LIBLEPT_DIR=/usr/local
RUN cd /tmp && \
    wget https://github.com/DanBloomberg/leptonica/releases/download/1.83.1/leptonica-1.83.1.tar.gz && \
    tar -xvzf leptonica-1.83.1.tar.gz && \
    cd leptonica-1.83.1 && \
    ./configure --prefix=$LIBLEPT_DIR && \
    make -j4 && \
    make install

# Set environment so Tesseract can find Leptonica
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV LD_LIBRARY_PATH=/usr/local/lib

# Build Tesseract
RUN cd /tmp && \
    wget https://github.com/tesseract-ocr/tesseract/archive/refs/tags/5.3.3.tar.gz -O tesseract.tar.gz && \
    tar -xvzf tesseract.tar.gz && \
    cd tesseract-5.3.3 && \
    ./autogen.sh && \
    ./configure --prefix=/usr/local && \
    make -j4 && \
    make install

# Add Hebrew + English traineddata
RUN mkdir -p /usr/local/share/tessdata && \
    cd /usr/local/share/tessdata && \
    wget https://github.com/tesseract-ocr/tessdata/raw/main/eng.traineddata && \
    wget https://github.com/tesseract-ocr/tessdata/raw/main/heb.traineddata

ENV TESSDATA_PREFIX=/usr/local/share/tessdata/

# Copy and install Python dependencies
COPY analyze_file/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# copy certs pack
COPY common/certs/roots/*.crt /opt/roots/

# Copy function code
COPY analyze_file/ ${LAMBDA_TASK_ROOT}/aws/analyze_file/
COPY common/ ${LAMBDA_TASK_ROOT}/aws/common/
COPY client_config.json ${LAMBDA_TASK_ROOT}/aws/analyze_file/client_config.json

CMD ["aws.analyze_file.lambda_function.lambda_handler"]
