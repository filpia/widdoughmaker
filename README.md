# Goals:
 - Investigate what whiskey asset classes malt/grain/first-fill/refill/etc, distilleries, and ages of product are most ripe for investment and probable gain
 
# Tools:
The WIDDoughMaker fetches assets (aka pitches) and their respective price histories. Afterwards, exploratory data analysis via python

# Infrastructure Overview

1. Code in docker container for hitting API to fetch current rates

2. Schedule docker container to execute as a lambda, outpput written to `s3://wid-prices`

3. Use `etl/stack_records_wide_to_long.py` to transform the data from wide to long format, writing the output to `s3://wid-prices-processed`

4. Configure AWS Glue crawler to traverse the `s3://wid-prices-processed` bucket, making the contents queryable on-demand. See configuration in section below

5. Add partitions? 

# Sample price chart
[Click me](https://www.whiskyinvestdirect.com/tullibardine/2015/Q4/BBF/chart.do)

# Building and Testing Docker Image for Lambda
1. Populate `config/configuration.txt` with Whiskey credentials

2. Populate `config/aws_config.txt` with AWS IAM creds

3. Temporarily uncomment lines in `Dockerfile` to mount AWS credentials. This isn't necessary in production as
command will be run in AWS lambda where credentials are part of the sesion
```Dockerfile
# ENV AWS_CONFIG_FILE=/config/aws/config
# COPY config/aws_config.txt /config/aws/config
```

4. From repo root, run
`docker build . -t widdoughmaker:YOUR_TAG`

5. Run docker image
`docker run -it --rm -p 9000:8080 widdoughmaker:YOUR_TAG`

6. Curl port running images
`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'`

7. Comment out lines in Dockerfile from item 3

8. Authenticate Docker to AWS ECR.
**Note:** Assumes your `~/.aws/config` is properly set to access your aws environment 

`aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 400419513456.dkr.ecr.us-east-1.amazonaws.com`

9. Create ECR Repository if it doesn't already exist. Follow instructions for configuring ECR repository for Lambda [here](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-images.html#configuration-images-update).
`aws ecr create-repository --repository-name widdoughmaker --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE --profile fil_personal`

10. Tag Docker image to match repository name
`docker tag  widdoughmaker:YOUR_TAG 400419513456.dkr.ecr.us-east-1.amazonaws.com/widdoughmaker:YOUR_TAG`

11. Push Docker Image to ECR
`docker push 400419513456.dkr.ecr.us-east-1.amazonaws.com/widdoughmaker:YOUR_TAG`
12. In the Lambda console, choose to deploy a new image. Make sure to specify the correct architecture for your image
either arm64 or x86

# Configuring an AWS Glue Crawler

Some AWS role set up is required but here is a screenshot of the crawler configuration

![Glue]('images/aws_glue_crawler_config.png')

