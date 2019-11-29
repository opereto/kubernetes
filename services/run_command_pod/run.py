from opereto.helpers.services import ServiceTemplate
from kubernetes_api import KubernetesAPI
from opereto.utils.validations import JsonSchemeValidator, validate_dict, default_variable_pattern, default_variable_name_scheme, item_properties_scheme
from opereto.exceptions import OperetoRuntimeError
import json

from pyopereto.client import OperetoClient, OperetoClientError
import time


class ServiceRunner(ServiceTemplate):

    def __init__(self, **kwargs):
        self.client = OperetoClient()
        ServiceTemplate.__init__(self, **kwargs)

    def validate_input(self):

        self.pod_template = self.input['pod_template']
        print 'Pod template:\n{}'.format(json.dumps(self.pod_template, indent=4))

        input_scheme = {
            "type": "object",
            "properties": {
                "pod_template": {
                    "type": "object"
                },
                "required": ['pod_template'],
                "additionalProperties": True
            }
        }

        validator = JsonSchemeValidator(self.input, input_scheme)
        validator.validate()

        if self.pod_template["metadata"]["name"].startswith('opereto-worker-node'):
            raise OperetoRuntimeError(error='Pod name is invalid, this name is used for Opereto standard elastic workers. Please select a different name.')


    def process(self):
        SUCCESS=False
        my_timeout = self.client.get_process_info()['timeout']-30
        print 'Running worker pod..'
        try:
            pod_name = self.pod_template['metadata']['name']
            self.kubernetes_api.create_pod(self.pod_template)
            runtime = my_timeout
            while runtime > 0:
                resp = self.kubernetes_api.get_pod(pod_name)
                if resp.status.phase != 'Running':
                    if resp.status.phase == 'Succeeded':
                        SUCCESS=True
                    break
                time.sleep(5)
                runtime -= 5
        except Exception,e:
            print str(e)
            return self.client.ERROR
        finally:
            try:
                print self.kubernetes_api.get_pod_log(pod_name)
                self.kubernetes_api.delete_pod(pod_name)
            except:
                pass

        if SUCCESS:
            return self.client.SUCCESS
        return self.client.FAILURE

    def setup(self):
        self.kubernetes_api = KubernetesAPI()

    def teardown(self):
        pass


if __name__ == "__main__":
    exit(ServiceRunner().run())
