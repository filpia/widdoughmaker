# using arm64 as building on M1 Mac
FROM public.ecr.aws/lambda/python:3.11-arm64

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
#RUN  pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

RUN mkdir -p  /config

ENV LAMBDA_TASK_ROOT=/var/task
ENV CONFIG_PATH=/config/configuration.txt

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY config/configuration.txt ${CONFIG_PATH}
COPY wid_tools.py ${LAMBDA_TASK_ROOT}

# When testing in local docker container, mount aws config to test push to s3
# ENV AWS_CONFIG_FILE=/config/aws/config
# COPY config/aws_config.txt /config/aws/config


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]

