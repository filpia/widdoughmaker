# Goals:
 - Investigate what whiskey asset classes malt/grain/first-fill/refill/etc, distilleries, and ages of product are most ripe for investment and probable gain
 
# Tools:
The WIDDoughMaker fetches assets (aka pitches) and their respective price histories. Afterwards, exploratory data analysis via python

# Sample price chart
[Click me](https://www.whiskyinvestdirect.com/tullibardine/2015/Q4/BBF/chart.do)

# Building and Testing Docker Image for Lambda
1. From repo root, run
`docker build . -t <TAG>`

2. Run docker image
`docker run -it --rm -p 9000:8080 <TAG>`

3. Curl port running images
`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'`


TODO:
- Create ECR
Link here: https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#images-create-from-base
Link about tagging: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-images.html#configuration-images-update
- push to ECR
- test in lambda
