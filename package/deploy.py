import os
from pyopereto.client import OperetoClient
from pyopereto.helpers.packages import OperetoAwsS3PackagesManager

package_directory = os.path.dirname(os.getcwd())

## set the AWS S3 bucket name, access key and secret key
client = OperetoClient()
opereto_packages_repo_ak = client.input['opereto_packages_repo_ak']
opereto_packages_repo_sk = client.input['opereto_packages_repo_sk']
opereto_packages_bucket_name = client.input['opereto_packages_bucket_name']

if __name__ == "__main__":
    obj = OperetoAwsS3PackagesManager(package_directory, opereto_packages_bucket_name, opereto_packages_repo_ak,opereto_packages_repo_sk, 'opereto-kubernetes-services')
    obj.deploy()


