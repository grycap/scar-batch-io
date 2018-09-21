#!/usr/bin/python
import boto3
import botocore
from botocore.exceptions import ClientError
import os
import errno
class S3():
    def __init__(self):
        self.client = boto3.client('s3')
    
    def download_bucket(self,bucket):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(bucket)
        for object in my_bucket.objects.all():
            print "Object : "+str(object)
            print "key : "+str(object.key)
            path= os.path.dirname(os.path.join(os.environ['SCAR_INPUT_DIR'], object.key))
            print "path : "+path
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

        print os.listdir(os.environ['SCAR_INPUT_DIR'])

    def download_file(self,bucket):
        try:
            head, tail = os.path.split(os.environ['SCAR_INPUT_FILE'])
            self.client.download_file(bucket, tail,os.environ['SCAR_INPUT_DIR']+"/"+tail)
            return tail
        except ClientError as ce:
            error_msg = "Error download file to S3."
            print error_msg, error_msg + ": %s" % ce
            raise ce
    def uploadDirectory(self,path,bucketname):
        try:
            for root,dirs,files in os.walk(path):
                for file in files:
                    key= bucketname+"/output/"+file
                    print key
                    self.client.put_object(Bucket=bucketname,Key=key,Body=file)
        except ClientError as ce:
            error_msg = "Error upload file to S3."
            print error_msg, error_msg + ": %s" % ce
            raise ce
        
    def _mkdir_recursive(self, path):
        sub_path = os.path.dirname(path)
        if not os.path.exists(sub_path):
            self._mkdir_recursive(sub_path)
        if not os.path.exists(path):
            os.mkdir(path)


def is_variable_in_environment(variable):
    return check_key_in_dictionary(variable, os.environ)

def check_key_in_dictionary(key, dictionary):
    return (key in dictionary) and dictionary[key] and dictionary[key] != ""


if __name__ == "__main__":
    s3 = S3()
    bucket = os.environ['NAME_FUNCT']
    if(os.environ['MODE']=="INIT"):
        with open(os.path.join(os.environ['SCAR_INPUT_DIR'],"script.sh"), "w") as file1:
            file1.write(os.environ['SCRIPT'])
            file1.close()
        os.system('chmod +x '+os.environ['SCAR_INPUT_DIR']+"/script.sh")
        print 'BUCKET_INPUT: '+str(os.environ['BUCKET_INPUT'])
        if is_variable_in_environment('BUCKET_INPUT'):
            if (os.environ['BUCKET_INPUT']!="NO"):
                s3.download_bucket(os.environ['BUCKET_INPUT'])
    elif(os.environ['MODE']=="FINISH"):
        if is_variable_in_environment('BUCKET_OUTPUT'):
            if (os.environ['BUCKET_OUTPUT']!="NO"):
                bucket = os.environ['BUCKET_OUTPUT']
        s3.uploadDirectory(os.environ['SCAR_OUTPUT_DIR'], bucket)
