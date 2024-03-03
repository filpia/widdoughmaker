# In-progress
- todo: schedule a lambda to process last month's raw files
- todo: send email on failure

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

# Seting up Local Environment with AWS Creds

1. Create a file in the following format
```
[default]
aws_access_key_id = string
aws_secret_access_key = string
region = us-east-1
output = json
```

2. Specify the path to that file as the environment variable `AWS_CONFIG_FILE`
```bash
export AWS_CONFIG_FILE=string
```

# Transform Records From Wide to Long

# Sample price chart
[Click me](https://www.whiskyinvestdirect.com/tullibardine/2015/Q4/BBF/chart.do)

# Building and Testing Docker Image for Lambda

## The Short Way


## The Long Way
1. Populate `config/configuration.txt` with Whiskey credentials

2. Populate `config/aws_config.txt` with AWS IAM creds

3. Temporarily uncomment lines in `Dockerfile` to mount AWS credentials. This isn't necessary in production as
command will be run in AWS lambda where credentials are part of the sesion
```Dockerfile
# ENV AWS_CONFIG_FILE=/config/aws/config
# COPY config/aws_config.txt /config/aws/config
```

4. From repo root, run
`./bin/build_and_push.sh YOUR_TAG`

5. Run docker image
`docker run -it --rm -p 9000:8080 widdoughmaker:YOUR_TAG`

6. To test, curl port running images
`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'`

7. Comment out lines in Dockerfile from item 3

8. In the Lambda console, choose to deploy a new image. Make sure to specify the correct architecture for your image
either arm64 or x86

# Configuring an AWS Glue Crawler

Some AWS role set up is required but here is a screenshot of the crawler configuration

![Glue]('images/aws_glue_crawler_config.png')

