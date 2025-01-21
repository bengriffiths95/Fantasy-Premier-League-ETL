import boto3

region = 'eu-west-2'
ec2_instance = ['i-09a5211bae0615273']

ec2 = boto3.client('ec2', region_name=region)

def lambda_handler(event, context):
    ec2.stop_instances(InstanceIds=ec2_instance)
    print('Stopped your instances: ' + str(ec2_instance))