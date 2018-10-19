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
import boto3
import botocore
from botocore.exceptions import ClientError
import os
import errno

class S3():
    
    def __init__(self):
        self.client = boto3.client('s3')
    
    def download_bucket(self, bucket):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(bucket)
        for object in my_bucket.objects.all():
            print("Object : {0}".format(object))
            print("Key : {0}".format(object.key))
            path = os.path.dirname(os.path.join(os.environ['SCAR_INPUT_DIR'], object.key))
            print("path : {0}".format(path))
            if not os.path.exists(path):
                try:
                    self._mkdir_recursive(path)
                except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise
            head, tail = os.path.split(os.path.join(os.environ['SCAR_INPUT_DIR'], object.key))
            if tail:
                with open(os.path.join(os.environ['SCAR_INPUT_DIR'], tail), 'wb') as data:
                    self.client.download_fileobj(bucket, object.key, data)     
        print(os.listdir(os.environ['SCAR_INPUT_DIR']))

    def download_file(self, bucket):
        try:
            head, tail = os.path.split(os.environ['SCAR_INPUT_FILE'])
            self.client.download_file(bucket, tail,os.environ['SCAR_INPUT_DIR']+"/"+tail)
            return tail
        except ClientError as ce:
            error_msg = "Error download file to S3."
            print(error_msg, "{0}: {1}".format(error_msg, ce))
            raise ce
        
    def uploadDirectory(self, path, bucketname):
        try:
            for root,dirs,files in os.walk(path):
                for file in files:
                    key = "{0}/output/{1}".format(bucketname, file)
                    print(key)
                    self.client.put_object(Bucket=bucketname ,Key=key, Body=file)
        except ClientError as ce:
            error_msg = "Error upload file to S3."
            print (error_msg, "{0}: {1}".format(error_msg, ce))
            raise ce
        
    def _mkdir_recursive(self, path):
        sub_path = os.path.dirname(path)
        if not os.path.exists(sub_path):
            self._mkdir_recursive(sub_path)
        if not os.path.exists(path):
            os.mkdir(path)

def is_variable_in_environment(variable):
    return variable in os.environ and os.environ[variable] and os.environ[variable] != ""

if __name__ == "__main__":
    s3 = S3()
    bucket = os.environ['FUNCTION_NAME']
    mode = os.environ['MODE']
    
    if mode == "INIT":
        with open(os.path.join(os.environ['SCAR_INPUT_DIR'], "script.sh"), "w") as file:
            file.write(os.environ['SCRIPT'])
        os.system('chmod +x {0}/script.sh'.format(os.environ['SCAR_INPUT_DIR']))
        
        print('INPUT_BUCKET: {0}'.format(os.environ['INPUT_BUCKET']))
        if is_variable_in_environment('INPUT_BUCKET'):
            s3.download_bucket(os.environ['INPUT_BUCKET'])
    
    elif mode == "END":
        if is_variable_in_environment('OUTPUT_BUCKET'):
            bucket = os.environ['OUTPUT_BUCKET']
            s3.uploadDirectory(os.environ['SCAR_OUTPUT_DIR'], bucket)
