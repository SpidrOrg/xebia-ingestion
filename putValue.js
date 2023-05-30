const fs = require("fs");
const path = require("path");
const { exec } = require('node:child_process');
// This
const indexOfAwsAccountInArnSplit = process.env.CODEBUILD_BUILD_ARN.split(":").indexOf(process.env.AWS_REGION) + 1;
const awsAccount = process.env.CODEBUILD_BUILD_ARN.split(":")[indexOfAwsAccountInArnSplit];
const awsRegion = process.env.AWS_REGION;
// or this//
// const awsAccount = "319925118739";
// const awsRegion = "us-east-1";


///
exec(`export bucket_name=${awsAccount}-codebase`);
exec(`export lambda_functions=[
  {
    "name": "lambda_function.py",
    "folder": "functions/moodys-monthly",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:krny-moodys-monthly"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/moodys-quarterly",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:krny-moodys-quarterly"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/moodys-yearly",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:krny-moodys-yearly"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/moodys-manual",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:krny-moodys-188"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/covid",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-covid"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/yahoo_function",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-yahoofin"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/fred",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-fred"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/googletrendschild",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-googletrends-child"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/googletrendsclient",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-googletrends-client"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/googletrendsparent",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-googletrends-parent"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/ihs",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-ihs"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/similar-web",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-similarweb"
  },
  {
    "name": "lambda_function.py",
    "folder": "functions/similar-web-client",
    "arn": "arn:aws:lambda:${awsRegion}:${awsAccount}:function:ingestion-similarweb-client"
  }
]
`)
