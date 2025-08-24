FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir --upgrade pip

WORKDIR ${LAMBDA_TASK_ROOT}

COPY lambda_search_checks/requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

COPY lambda_search_checks/ ${LAMBDA_TASK_ROOT}/aws/lambda_search_checks/
COPY common/ ${LAMBDA_TASK_ROOT}/aws/common/
COPY client_config.json ${LAMBDA_TASK_ROOT}/aws/analyze_file/client_config.json

CMD ["aws.lambda_search_checks.lambda_function.lambda_handler"]
