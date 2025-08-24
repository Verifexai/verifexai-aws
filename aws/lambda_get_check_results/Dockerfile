FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir --upgrade pip

WORKDIR ${LAMBDA_TASK_ROOT}

COPY lambda_get_check_results/requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

COPY lambda_get_check_results/ ${LAMBDA_TASK_ROOT}/aws/lambda_get_check_results/
COPY common/ ${LAMBDA_TASK_ROOT}/aws/common/

CMD ["aws.lambda_get_check_results.lambda_function.lambda_handler"]
