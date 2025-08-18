FROM public.ecr.aws/lambda/python:3.12

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements and install
COPY lambda_ingest_and_classify/requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY lambda_ingest_and_classify/ ${LAMBDA_TASK_ROOT}/aws/lambda_ingest_and_classify/
COPY common/ ${LAMBDA_TASK_ROOT}/aws/common/

CMD ["aws.lambda_ingest_and_classify.lambda_function.lambda_handler"]