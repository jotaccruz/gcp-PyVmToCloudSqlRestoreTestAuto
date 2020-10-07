#Service Account: mssql-restore-test@ti-is-devenv-01.iam.gserviceaccount.com
#SET GOOGLE_APPLICATION_CREDENTIALS=C:\pythonVE\gcp-alerts-management\ti-is-devenv-01-e494bc35aeae.json
#Storage Admin over specfic Bucket

#gcloud beta sql import bak sql1 gs://dba-freenas/SUSWEYAK15_EvoDb_Testing_FULL_20200325_011850.bak --database=EvoDb_Testing
import dbconn
from dbconn import *

import argparse
import os
import time

from six.moves import input

from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json

import random
import string

# [START add_bucket_iam_member]
# To get bucket necessary permissions for the interacting with the Cloud SQL Admin API.
def add_bucket_iam_member(bucket_name, role, member):
    """Add a new member to an IAM Policy"""
    # bucket_name = "your-bucket-name"
    # role = "IAM role, e.g. roles/storage.objectViewer"
    # member = "IAM identity, e.g. user: name@example.com"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.bindings.append({"role": role, "members": {member}})
    bucket.set_iam_policy(policy)
    print("Added {} with role {} to {}.".format(member, role, bucket_name))
# [END add_bucket_iam_member]


# [START list_sql_instances]
def list_sql_instances(cloudsql,projectname,sqlinstance_name):
	req = cloudsql.instances().list(project=projectname)
	resp = req.execute()
	sqlinstances = {'instance': 'Active'}
	for key in resp:
		for items in resp[key]:
			if items['name'].startswith(sqlinstance_name):
				sqlinstances['name'] = items['name']
				sqlinstances['email'] = items['serviceAccountEmailAddress']
				EmailAddress=items['serviceAccountEmailAddress']
				if items['ipAddresses'][0]['type']=='PRIMARY':
					sqlinstances['ip'] = [items['ipAddresses'][0]['ipAddress']]
	#https://cloud.google.com/sql/docs/sqlserver/import-export/importing
	add_bucket_iam_member("dba-freenas","roles/storage.admin","serviceAccount:" + EmailAddress)
	return sqlinstances
# [END list_sql_instances]


# [START destroy_sqlinstance]
def destroy_sqlinstance(cloudsql, projectname,sqlinstance_name):
	return cloudsql.instances().delete(project=projectname,instance=sqlinstance_name).execute()
# [END destroy_sqlinstance]

#destroy_sqlinstances("ti-is-devenv-01","sql1")


# [START wait_for_operation]
def wait_for_operation(cloudsql, project, operation):
    print('Waiting for operation to finish...')
    while True:
        result = cloudsql.operations().get(
            project=project,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result
        time.sleep(1)
# [END wait_for_operation]
#wait_for_operation(cloudsql, "ti-is-devenv-01", operation)

# [START get_random_string]
def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
# [END get_random_string]

# [START generate_random_name]
def generate_random_name(sqlinstance_name,length):
	sqlinstance_name = sqlinstance_name + "-" + get_random_string(length)
	return sqlinstance_name
# [END generate_random_name]

# [START create_instance]
def create_sqlinstance(cloudsql, project, zone, sqlinstance_name, machine_type, ssd_size, sqlversion, saPasswd):
    # Configure the SQL Instance
    config = {
        'name': sqlinstance_name,
        'gceZone': zone,
        'region': zone[0:8],
        'databaseVersion': sqlversion,
        'rootPassword': saPasswd,
        'settings': {
            'locationPreference':{
                'zone': zone
            },
            'userLabels': {
                'owner': 'dba',
                'purpose': 'restore_test_automation'
            },
            'tier': machine_type,
            'dataDiskSizeGb': ssd_size,
            'ipConfiguration': {
                'authorizedNetworks': [
                    {
                        'value': '208.181.137.109',
                        #"expirationTime": '2021-10-02T15:01:23Z',
                        'name': 'VDI'
                    }
                ]
            },
        },
    }
    return cloudsql.instances().insert(
        project=project,
        body=config).execute()
# [END create_instance]
#create_sqlinstance(cloudsql,"ti-is-devenv-01","us-west1-a",generate_random_name(5),"db-custom-4-15360",100,'SQLSERVER_2017_WEB',"Pass12345")


# [START import_instance]
def import_sqlinstance(cloudsql, project, sqlinstance_name,database_name,filetype):
    config = {
        'importContext': {
            'uri': 'gs://dba-freenas/SUSWEYAK15_EvoDb_Testing_FULL_20200325_011850.bak',
            'database': database_name,
            'fileType': filetype
        }
    }

    return cloudsql.instances().import_(
    	project=project,
    	instance=sqlinstance_name,
    	body=config).execute()
# [END import_instance]
#import_sqlinstance(cloudsql,"ti-is-devenv-01","us-west1-a",instances['targetID'],'EvoDb_Testing','BAK')


# [START run]
def main(project, bucket, zone, sqlinstance_name, wait=True):
    # Construct the service object for the interacting with the Cloud SQL Admin API.
    cloudsql = build('sqladmin','v1beta4')

    print('Creating the CloudSQL Instance...')

    operation = create_sqlinstance(cloudsql,"ti-is-devenv-01","us-west1-a",generate_random_name(sqlinstance_name,5),"db-custom-4-15360",100,'SQLSERVER_2017_WEB',"Pass12345")
    wait_for_operation(cloudsql, project, operation['name'])

    print("""
CloudSQL Instance created.
It will take a minute or two for the instance to complete work.
Once the CloudSQL Instance is created press enter to IMPORT a database.
""".format(project))

    instances = list_sql_instances(cloudsql, project,operation['targetId'])

    print('CloudSQL Instances in project %s and zone %s:' % (project, zone))
    print(instances['name'])
    print(instances['email'])
    print(instances['ip'])

    if wait:
        input()

    print('Importing a Database.')

    operation = import_sqlinstance(cloudsql, project, instances['name'],'EvoDb_Testing','BAK')
    wait_for_operation(cloudsql, project, operation['name'])


    print("""
Database imported.
Once the Database is imported press enter to DELETE the instance.
""".format(project))

    if wait:
        input()

    print('Deleting CloudSQL instance.')

    operation = destroy_sqlinstance(cloudsql, project, instances['name'])
    wait_for_operation(cloudsql, project, operation['name'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument('bucket_name', help='Your Google Cloud Storage bucket name.')
    parser.add_argument('--zone', default='us-west1-a', help='Cloud SQL zone to deploy to.')
    parser.add_argument('--name', default='sql1', help='New instance name.')

    args = parser.parse_args()

    main(args.project_id, args.bucket_name, args.zone, args.name)
# [END run]
#python sqlinstance.py --name sqlrestore --zone us-west1-a ti-is-devenv-01 dba-freenas








#Using credentials
#-------------------------------------------------------------------------------------------------------
# Construct the service object for the interacting with the Cloud SQL Admin API.
def list_sql_instances_cred(projectname,):
	credentials = service_account.Credentials.from_service_account_file('')

	scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])

	service = build('sqladmin', 'v1beta4',credentials=credentials)

	req = service.instances().list(project=projectname)
	resp = req.execute()
	for key in resp:
		for items in resp[key]:
			if items['name']=='sql1':
				print (items['name'])
				print (items['serviceAccountEmailAddress'])
				if items['ipAddresses'][0]['type']=='PRIMARY':
					print (items['ipAddresses'][0]['ipAddress'])
