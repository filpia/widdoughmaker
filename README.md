# Goals:
 - Investigate what whiskey asset classes malt/grain/first-fill/refill/etc, distilleries, and ages of product are most ripe for investment and probable gain
 
# Tools:
The WIDDoughMaker fetches assets (aka pitches) and their respective price histories. Afterwards, exploratory data analysis via python

# Sample price chart
[Click me](https://www.whiskyinvestdirect.com/tullibardine/2015/Q4/BBF/chart.do)

# Building and Testing Docker Image for Lambda
1. From repo root, run
`docker build . -t widdoughmaker`

2. Run docker image
`docker run -it --rm -p 9000:8080 widdoughmaker`

3. Curl port running images
`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'`

4. Authenticate Docker to AWS ECR
`aws ecr get-login-password --region us-east-1 --profile fil_personal | docker login --username AWS --password-stdin 400419513456.dkr.ecr.us-east-1.amazonaws.com`

5. Create ECR Repository if it doesn't already exist
`aws ecr create-repository --repository-name widdoughmaker --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE --profile fil_personal`

6. Tag Docker image to match repository name
`docker tag  widdoughmaker:latest 400419513456.dkr.ecr.us-east-1.amazonaws.com/widdoughmaker:latest`

7. Push Docker Image to ECR
`docker push 400419513456.dkr.ecr.us-east-1.amazonaws.com/widdoughmaker:latest`

TODO:
- Create ECR
Link here: https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#images-create-from-base
Link about tagging: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-images.html#configuration-images-update
- push to ECR
- test in lambda
