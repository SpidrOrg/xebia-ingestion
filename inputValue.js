const fs = require("fs");
const path = require("path");

// This
const indexOfAwsAccountInArnSplit = process.env.CODEBUILD_BUILD_ARN.split(":").indexOf(process.env.AWS_REGION) + 1;
const awsAccount = process.env.CODEBUILD_BUILD_ARN.split(":")[indexOfAwsAccountInArnSplit];
const awsRegion = process.env.AWS_REGION;
// or this//
// const awsAccount = "319925118739";
// const awsRegion = "us-east-1";

const TERRAFORM_STATEFILE_BUCKET = process.env.TERRAFORM_STATEFILE_BUCKET;
const TERRAFORM_STATELOCK_DD_TABLE = process.env.TERRAFORM_STATELOCK_DD_TABLE;

const TOKENS = {
//   ":TERRAFORM_STATEFILE_BUCKET:": TERRAFORM_STATEFILE_BUCKET,
//   ":TERRAFORM_STATELOCK_DD_TABLE:": TERRAFORM_STATELOCK_DD_TABLE,
//   ":ENV_NAME:": "stage",
//   ":ENVIRONMENT123123:": process.env.ENV_NAME,
  ":123456789012:": awsAccount
}

console.log(TOKENS)
const replaceFilePath = path.join(__dirname, "./envs/stage/terragrunt.hcl");
let fileContents = fs.readFileSync(replaceFilePath, "utf-8");
Object.keys(TOKENS).forEach(token=>{
  fileContents = fileContents.replaceAll(token, TOKENS[token]);
})
fs.writeFileSync(replaceFilePath, fileContents);

function recursivelyReplaceTokens(startPath){
  const foldersOrFiles = fs.readdirSync(startPath).filter(item => !/(^|\/)\.[^/.]/g.test(item))
  foldersOrFiles.forEach((folderOrFile)=>{
    const folderOrFilePath = path.join(startPath, folderOrFile)
    const isFolder = fs.lstatSync(folderOrFilePath).isDirectory();
    if (isFolder){
      recursivelyReplaceTokens(folderOrFilePath);
    } else {
      let fileContents = fs.readFileSync(folderOrFilePath, "utf-8");
      Object.keys(TOKENS).forEach(token=>{
        fileContents = fileContents.replaceAll(token, TOKENS[token]);
      })
      fs.writeFileSync(folderOrFilePath, fileContents);
    }
  })
}

const pathToInfraFolder = path.join(__dirname, "./envs/stage");
recursivelyReplaceTokens(pathToInfraFolder);
