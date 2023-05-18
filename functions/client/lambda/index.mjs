import {S3Client} from "@aws-sdk/client-s3"
import _ from "lodash";
import {STSClient} from "@aws-sdk/client-sts";
import {AssumeRoleCommand} from "@aws-sdk/client-sts";
import transformationConfig from "./transformationConfig.mjs";
import {isFilePresent, readFileAsString, writeFileToS3} from "./s3Utils.mjs";
import {createProgrammingStruct, findDifference, convertToFileContents} from "./utils.mjs";
import transformer from "./transformer.mjs";

const REGION = "us-east-1";
const ACCOUNT_ID = '287882505924'
const EXECUTION_ROLE_NAME = 'client-data-transformation-execution-role'
const ASSUME_ROLE_TAG = 'BucketName'

const stsClient = new STSClient({region: REGION});

function findTransformationFromEvent(event){
  const fileKey = _.get(event, "Records[0].s3.object.key");
  return _.reduce(_.values(transformationConfig), (acc, v)=>{
    if(!acc && v.rawFileKey(event) === fileKey){
      return v;
    }
    return acc;
  }, null)
}

export const handler = async (event) => {
  //get the bucket name
  const s3bucket = _.get(event,"Records[0].s3.bucket.name");
  
  console.log("s3bucket", s3bucket);

  try {
    const command = new AssumeRoleCommand({
      RoleArn: `arn:aws:iam::${ACCOUNT_ID}:role/${EXECUTION_ROLE_NAME}`,
      RoleSessionName: `assume-${s3bucket}`,
      DurationSeconds: 900,
      Tags: [{
        'Key': ASSUME_ROLE_TAG,
        'Value': s3bucket
      }]
    });
    const stsResponse = await stsClient.send(command);
    
    const stsCredentials = _.get(stsResponse, "Credentials");
    
    const s3Client = new S3Client({
      region: REGION, credentials: {
        accessKeyId: _.get(stsCredentials, "AccessKeyId"),
        expiration: _.get(stsCredentials, "Expiration"),
        secretAccessKey: _.get(stsCredentials, "SecretAccessKey"),
        sessionToken: _.get(stsCredentials, "SessionToken")
      }
    });
    
    const transformationConfiguration = findTransformationFromEvent(event);
  
    if (_.isEmpty(transformationConfiguration)){
      console.log("No match config found, returning...")
      return
    }
    let {rawFileKey, transformFileKey, primaryKeyIndexes, lineTransformationConfig} = transformationConfiguration
  
    const rawFKey = () => rawFileKey(event);
    const transformFKey = () => transformFileKey(event);
    // Read file from S3 entirely
    const lhsFileContents = await readFileAsString(s3Client, s3bucket, rawFKey);
  
    // Transform file
    let transformedLHSFileContents = transformer(lhsFileContents, lineTransformationConfig);
  
    // Check if transformed file exits - if not exits then do write the tranformed data as file to tranformed location
    // else do comparision and write.
    const isFileFound = await isFilePresent(s3Client, s3bucket, transformFKey)
  
    if (isFileFound) {
      // Pull the RHS file
      const rhsFileContents = await readFileAsString(s3Client, s3bucket, transformFKey);
  
      // Create the programming struct from the RHS file
      const RHS_file_programming_struct = createProgrammingStruct(rhsFileContents, primaryKeyIndexes);
  
      // Create the programming struct from the LHS file
      const LHS_file_programming_struct = createProgrammingStruct(transformedLHSFileContents, primaryKeyIndexes);
  
      // We do comparison
      _.forEach(_.keys(LHS_file_programming_struct), lhsPrimaryKey => {
        const isRhsHasTheKey = RHS_file_programming_struct[lhsPrimaryKey];
        if (isRhsHasTheKey) {
          const diff = findDifference(_.get(RHS_file_programming_struct, `${lhsPrimaryKey}`), _.get(LHS_file_programming_struct, `${lhsPrimaryKey}`));
          if (_.size(diff) > 0) {
            RHS_file_programming_struct[lhsPrimaryKey] = [...RHS_file_programming_struct[lhsPrimaryKey], ...diff]
          }
        } else {
          RHS_file_programming_struct[lhsPrimaryKey] = LHS_file_programming_struct[lhsPrimaryKey];
        }
      });
      const transformedFileContent = convertToFileContents(RHS_file_programming_struct);
      await writeFileToS3(s3Client, s3bucket, transformFKey, transformedFileContent);
    } else {
      await writeFileToS3(s3Client, s3bucket, transformFKey, transformedLHSFileContents);
    }
    
  }catch(error){
    console.error(error)
  }
  

};
