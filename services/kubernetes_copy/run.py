import time, os, re
from kubernetes_api import KubernetesAPI
from opereto.helpers.services import ServiceTemplate
from opereto.utils.validations import JsonSchemeValidator

class ServiceRunner(ServiceTemplate):

    def __init__(self, **kwargs):
        ServiceTemplate.__init__(self, **kwargs)

    def validate_input(self):

        input_scheme = {
            "type": "object",
            "properties": {

                "worker_pod_id": {
                    "type": "string",
                    "minLength": 1
                },
                "worker_pod_path": {
                    "type": "string",
                    "minLength": 1
                },
                "current_path": {
                    "type": "string",
                    "minLength": 1
                },
                "copy_direction": {
                    "type": "string",
                    "enum": ['copy_from', 'copy_to']
                },
                "aggregation_interval": {
                    "type": "integer"
                },
                "required": ['copy_direction', 'worker_pod_id', 'worker_pod_path', 'opereto_worker_path'],
                "additionalProperties": True
            }
        }

        validator = JsonSchemeValidator(self.input, input_scheme)
        validator.validate()

    def process(self):

        current_path = self.input['opereto_workspace']
        if self.input['current_path']:
            current_path = self.input['current_path']

        if self.input['copy_direction']=='copy_from':
            self._print_step_title('Coping from worker pod {} to local directory..'.format(self.input['worker_pod_id']))
        else:
            self._print_step_title('Coping from local directory to worker pod {} ..'.format(self.input['worker_pod_id']))

        def _copy():
            ret = self.kubernetes_api.cp(self.input['worker_pod_id'], self.input['worker_pod_path'], current_path,
                                         direction=self.input['copy_direction'])
            return ret

        if self.input['aggregation_interval']>0:
            print('Copy every {} seconds..'.format(self.input['aggregation_interval']))
            while(True):
                ret=_copy()
                if ret != 0:
                    print('Failed to copy: Exit code is: {}'.format(exc))
        else:
            ret=_copy()
            if ret!=0:
                print('Exit code is: {}'.format(exc))
                return self.client.FAILURE
        return self.client.SUCCESS

    def setup(self):
        self.kubernetes_api = KubernetesAPI()

    def teardown(self):
        pass



if __name__ == "__main__":
    exit(ServiceRunner().run())
