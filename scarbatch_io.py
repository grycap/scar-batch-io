# Copyright (C) GRyCAP - I3M - UPV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import boto3
import logging
from urllib.parse import unquote_plus
import json
import base64

logger = logging.getLogger()

def lazy_property(func):
    ''' A decorator that makes a property lazy-evaluated.'''
    attr_name = '_lazy_' + func.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)
    return _lazy_property

class Lambda():

    def __init__(self, event):
        self.event = event
        self.input_folder = get_environment_variable('SCAR_INPUT_DIR')
        self.output_folder = get_environment_variable('SCAR_OUTPUT_DIR')
        self.request_id = get_environment_variable('REQUEST_ID')

    @lazy_property
    def output_bucket(self):
        output_bucket = get_environment_variable('OUTPUT_BUCKET')
        return output_bucket
    
    @lazy_property
    def output_bucket_folder(self):
        output_folder = get_environment_variable('OUTPUT_FOLDER')
        return output_folder
    
    @lazy_property
    def input_bucket(self):
        input_bucket = get_environment_variable('INPUT_BUCKET')
        return input_bucket
    
    def has_output_bucket(self):
        return is_variable_in_environment('OUTPUT_BUCKET')

    def has_output_bucket_folder(self):
        return is_variable_in_environment('OUTPUT_FOLDER')
    
    def has_input_bucket(self):
        return is_variable_in_environment('INPUT_BUCKET')

class S3():
    
    @lazy_property
    def client(self):
        client = boto3.client('s3')
        return client
    
    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        if is_value_in_dict(self.lambda_instance.event, 'Records'):
            self.record = self.get_s3_record()
            self.input_bucket = self.record['bucket']['name']
            self.file_key = unquote_plus(self.record['object']['key'])
            self.file_name = os.path.basename(self.file_key).replace(' ', '')
            self.file_download_path = '{0}/{1}'.format(self.lambda_instance.input_folder, self.file_name)
            self.function_name = os.path.dirname(self.file_key).split("/")[0]

    def get_s3_record(self):
        if len(self.lambda_instance.event['Records']) > 1:
            logger.warning("Multiple records detected. Only processing the first one.")
            
        record = self.lambda_instance.event['Records'][0]
        if is_value_in_dict(record, 's3'):
            return record['s3']

    def download_input(self):
        '''Downloads the file from the S3 bucket and returns the path were the download is placed'''
        print("Downloading item from bucket '{0}' with key '{1}'".format(self.input_bucket, self.file_key))
        if not os.path.isdir(self.file_download_path):
            os.makedirs(os.path.dirname(self.file_download_path), exist_ok=True)
        with open(self.file_download_path, 'wb') as data:
            self.client.download_fileobj(self.input_bucket, self.file_key, data)
        print("Successful download of file '{0}' from bucket '{1}' in path '{2}'".format(self.file_key,
                                                                                         self.input_bucket,
                                                                                         self.file_download_path))
        return self.file_download_path
  
    def get_file_key(self, function_name=None, folder=None, file_name=None):
        if function_name:
            return "{0}/{1}/{2}/{3}".format(function_name, folder, self.lambda_instance.request_id, file_name)
        else:
            return "{0}/{1}/{2}".format(folder, self.lambda_instance.request_id, file_name)

    def upload_output(self, bucket_name, bucket_folder=None):
        output_files_path = get_all_files_in_directory(self.lambda_instance.output_folder)
        print("UPLOADING FILES {0}".format(output_files_path))
        for file_path in output_files_path:
            file_name = file_path.replace("{0}/".format(self.lambda_instance.output_folder), "")
            if bucket_folder:
                file_key = self.get_file_key(folder=bucket_folder, file_name=file_name)
            else:
                file_key = self.get_file_key(function_name=self.function_name, folder='output', file_name=file_name)
            self.upload_file(bucket_name, file_path, file_key)
            
    def upload_file(self, bucket_name, file_path, file_key):
        print("Uploading file  '{0}' to bucket '{1}'".format(file_key, bucket_name))
        with open(file_path, 'rb') as data:
            self.client.upload_fileobj(data, bucket_name, file_key)
        print("Changing ACLs for public-read for object in bucket {0} with key {1}".format(bucket_name, file_key))
        obj = boto3.resource('s3').Object(bucket_name, file_key)
        obj.Acl().put(ACL='public-read')
    
    def download_file_to_memory(self, bucket_name, file_key):
        obj = boto3.resource('s3').Object(bucket_name=bucket_name, key=file_key)
        print ("Reading item from bucket {0} with key {1}".format(bucket_name, file_key))
        return obj.get()["Body"].read()
    
    def delete_file(self):
        self.client.delete_object(Bucket=self.input_bucket, Key=self.file_key)

def join_paths(*paths):
    return os.path.join(*paths)

def get_all_files_in_directory(dir_path):
    files = []
    for dirname, _, filenames in os.walk(dir_path):
        for filename in filenames:
            files.append(os.path.join(dirname, filename))
    return files

def set_log_level():
    if is_variable_in_environment('LOG_LEVEL'):
        logger.setLevel(get_environment_variable('LOG_LEVEL'))
    else:
        logger.setLevel('INFO')

def is_variable_in_environment(variable):
    return is_value_in_dict(os.environ, variable)

def get_environment_variable(variable):
    if is_variable_in_environment(variable):
        return os.environ[variable]

def is_value_in_dict(dictionary, value):
    return value in dictionary and dictionary[value]

def base64_to_utf8_string(value):
    return base64.b64decode(value).decode('utf-8')

def create_file_with_content(path, content):
    with open(path, "w") as f:
        f.write(content)

def create_user_script():
    if is_variable_in_environment('SCRIPT'):
        script_path = join_paths(get_environment_variable('SCAR_INPUT_DIR'), 'script.sh')
        script_content = base64_to_utf8_string(get_environment_variable('SCRIPT'))
        create_file_with_content(script_path, script_content)
        print("Script file created in '{0}'".format(script_path))
        os.system('chmod +x {0}'.format(script_path))
     
def parse_input():
    if is_variable_in_environment('INPUT_BUCKET'):
        lambda_instance = Lambda(json.loads(os.environ['LAMBDA_EVENT']))
        print('INPUT_BUCKET: {0}'.format(os.environ['INPUT_BUCKET']))
        S3(lambda_instance).download_input()    

def parse_output():
    upload_to_bucket()

def upload_to_bucket():
    lambda_instance = Lambda(json.loads(os.environ['LAMBDA_EVENT']))
    bucket_name = None
    bucket_folder = None

    if lambda_instance.has_output_bucket():
        bucket_name = lambda_instance.output_bucket
        print("OUTPUT BUCKET SET TO {0}".format(bucket_name))

        if lambda_instance.has_output_bucket_folder():
            bucket_folder = lambda_instance.output_bucket_folder
            print("OUTPUT FOLDER SET TO {0}".format(bucket_folder))

    elif lambda_instance.has_input_bucket():
        bucket_name = lambda_instance.input_bucket
        print("OUTPUT BUCKET SET TO {0}".format(bucket_name))

    if bucket_name:
        S3(lambda_instance).upload_output(bucket_name, bucket_folder)

if __name__ == "__main__":
    set_log_level()
    step = os.environ['STEP']
    if step == "INIT":
        print("INIT STEP")
        create_user_script()
        parse_input()
        
    elif step == "END":
        print("END STEP")
        parse_output()
