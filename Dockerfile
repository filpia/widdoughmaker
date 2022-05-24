FROM public.ecr.aws/lambda/python:3.8
#FROM continuumio/miniconda3:4.8.2

RUN mkdir -p  /config


ENV LAMBDA_TASK_ROOT=/var/task

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
# TODO: fix this here and in where code looks for this file
COPY config/configuration.txt /config/configuration.txt 
COPY wid_tools.py ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
#RUN  pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]

