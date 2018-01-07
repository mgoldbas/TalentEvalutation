
#!/usr/bin/env python
import os
import datetime
import subprocess
import argparse
import json
import boto3
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# Had so may problems with formatting all these arguments through the bash shell, so ...
# I'm trying out using Python's subprocess library to kick off the command.

sfx = datetime.datetime.now().strftime("%Y%m%d_%H")

STEPS_JSON="https://s3-sa-east-1.amazonaws.com/caserta-notebooks/bw/JupyterSteps.json"
ZEP_CONFIG="https://s3-sa-east-1.amazonaws.com/caserta-notebooks/bw/zep_config.json"

CMD_ARGS = ["aws","emr","create-cluster",
    "--release-label","emr-5.4.0",
    "--instance-groups","InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge,Name='Core instance group - 2'","InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge,Name='Master instance group - 1' ",
    "--ec2-attributes", "KeyName=billw1_keypair,InstanceProfile=EMR_EC2_DefaultRole,SubnetId=subnet-e5049ace,EmrManagedSlaveSecurityGroup=sg-c7d37bbf,EmrManagedMasterSecurityGroup=sg-c6d37bbe",
    "--service-role","EMR_DefaultRole",
    "--enable-debugging",
    "--log-uri","s3n://aws-logs-221927231121-us-east-1/elasticmapreduce/",
    "--name","skillseval_",
    "--applications", "Name=Hive","Name=Hadoop","Name=Spark",
    "--steps", STEPS_JSON,
    "--tags","Name=spk_node",
    "--region","us-east-1" ]

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--name", help="Name of candidate", default="Julius Erving")
    parser.add_argument("-d", "--date", help="Date candiate notified", default="20170118")

    args = vars(parser.parse_args())

    return args

def updateJupyterSteps(candidateName):

    bn = "caserta-notebooks"
    obj = "bw/JupyterSteps.json"

    # read the existing JupyterSteps.json file
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bn,Key=obj)
    contents = response['Body'].read()

    steps = json.loads(contents)

    # replace the parameter to the Run Post Setup Script with the Candidate's name
    steps[1]["Args"][2] = candidateName

    # write the file back out to S3
    fake_handle = StringIO(json.dumps(steps, indent=4, separators=(',', ': ')))
    s3_client.put_object(ACL="public-read",Bucket=bn, Key=obj, Body=fake_handle.read())

    return


def StartHere():
    print('Getting started ...')

    args = getArgs()

    # print("Args are {}".format(args))
    # print("CMD_ARGS are {}".format(CMD_ARGS))
    CMD_ARGS[16] += args['name'].replace(" ","")
    # print("Name item is: {}".format(CMD_ARGS[16]))

    # Update the JupyterSteps.json file with the Candidate's name
    updateJupyterSteps(args['name'].replace(" ",""))

    proc = subprocess.Popen(CMD_ARGS,stdout=subprocess.PIPE,)

    stdout_value = proc.communicate()[0]
    cmd_output = json.loads(stdout_value)

    print('The raw output was:',repr(stdout_value))
    print('ClusterId is: %s' % cmd_output['ClusterId'])

    print('All done.')
    return


if __name__ == "__main__":
    StartHere()