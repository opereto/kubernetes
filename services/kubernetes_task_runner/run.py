import json,yaml
import os
import re
import time
from opereto.helpers.services import TaskRunner
from kubernetes_api import KubernetesAPI
from opereto.utils.validations import JsonSchemeValidator, validate_dict, default_variable_pattern, default_variable_name_scheme, item_properties_scheme
from opereto.exceptions import OperetoRuntimeError


class ServiceRunner(TaskRunner):

    def __init__(self, **kwargs):
        TaskRunner.__init__(self, **kwargs)

    def _validate_input(self):

        self.pod_template = self.input['pod_template']
        self.output_file_path = self.input['output_file_path']

        input_scheme = {
            "type": "object",
            "properties": {
                "pod_config_files": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string"
                            },
                            "src": {
                                "type": "string"
                            },
                            "target": {
                                "type": "string"
                            }
                        }
                    },
                    "minItems": 0
                },
                "pod_template": {
                    "type": "object"
                },
                "output_file_path": {
                    "type": ["string", "null"]
                },
                "required": ['pod_template'],
                "additionalProperties": True
            }
        }

        validator = JsonSchemeValidator(self.input, input_scheme)
        validator.validate()

        try:
            if self.pod_template["metadata"]["name"].startswith('opereto'):
                raise OperetoRuntimeError(error='Pod names containing the prefix [opereto] are reserved names. Please select a different name.')
        except Exception as e:
            raise OperetoRuntimeError(error='Could not identify pod name: {}'.format(str(e)))


    def process(self):
        self.task_exitcode = self._run_task()
        self._wait_listener()
        return self.task_exitcode

    def _run_task(self):
        SUCCESS=False
        my_timeout = self.client.get_process_info()['timeout']-30

        if self.input['pod_config_files']:
            for config_file in self.input['pod_config_files']:
                configmap_name = re.sub('[^0-9a-z-]+', '-', config_file['name']+'-'+self.input['pid'].lower())
                self._print_step_title('Creating config file {}..'.format(configmap_name))
                configmap_data = config_file['data']
                if validate_dict(config_file['data']):
                    configmap_data = yaml.safe_dump(config_file['data'])
                print(self.kubernetes_api.create_config_map(configmap_name, {config_file['name']: configmap_data}))
                self._state['configmap'][configmap_name]={}
                self._save_state(self._state)
                self.config_maps[configmap_name]={
                    "target": config_file['target']
                }

            for configmap_name, config_attr in self.config_maps.items():
                if not 'volumes' in self.pod_template['spec']:
                    self.pod_template['spec']['volumes']=[]
                self.pod_template['spec']['volumes'].append(
                    {
                        "name": configmap_name+'-vol',
                        "configMap": {
                            "name": configmap_name
                        }
                    }
                )
                for i in range(len(self.pod_template['spec']['containers'])):
                    if not 'volumeMounts' in self.pod_template['spec']['containers'][i]:
                        self.pod_template['spec']['containers'][i]['volumeMounts']=[]
                    self.pod_template['spec']['containers'][i]['volumeMounts'].append(
                        {
                            "name": configmap_name+'-vol',
                            "mountPath": config_attr['target']
                        }
                    )

        ## add opereto worker sidecar container
        self.pod_template['spec']['containers'].append({
            "image": "opereto/worker",
            "name": self.test_container_name+"-opereto-worker",
            "resources": {
                "requests": {
                    "memory": "2Gi"
                },
                "limits": {
                    "memory": "2Gi"
                }
            },
            "env": [
                {
                    "name": "opereto_host",
                    "value": self.input['opereto_host']
                },
                {
                    "name": "opereto_user",
                    "value": self.input['opereto_user']
                },
                {
                    "name": "opereto_password",
                    "value": self.input['opereto_password']
                },
                {
                    "name": "agent_name",
                    "value": self.pod_name
                },
                {
                    "name": "javaParams",
                    "value": "-Xms500m -Xmx500m"
                },
                {
                    "name": "log_level",
                    "value": "info"
                }
            ]
        })

        if self.input['test_parser_config']:
            if not 'volumes' in self.pod_template['spec']:
                self.pod_template['spec']['volumes'] = []
            self.pod_template['spec']['volumes'].append(
                {
                    "name": 'opereto-test-results-directory',
                    "emptyDir": {}
                }
            )
            for i in range(len(self.pod_template['spec']['containers'])):
                if not 'volumeMounts' in self.pod_template['spec']['containers'][i]:
                    self.pod_template['spec']['containers'][i]['volumeMounts'] = []
                self.pod_template['spec']['containers'][i]['volumeMounts'].append(
                    {
                        "name": 'opereto-test-results-directory',
                        "mountPath": self.input['test_results_directory']
                    }
                )

        print 'Pod template:\n{}'.format(json.dumps(self.pod_template, indent=4))
        try:
            self._print_step_title('Running worker pod..')
            print(self.kubernetes_api.create_pod(self.pod_template))
            self._state['pod'][self.pod_name] = {}
            self._save_state(self._state)
            self._run_parser(self.pod_name)
            runtime = my_timeout
            while runtime > 0:
                resp = self.kubernetes_api.get_pod(self.pod_name)
                if resp.status.phase != 'Running':
                    if resp.status.phase == 'Succeeded':
                        SUCCESS=True
                    break
                else:
                    test_container_status=False
                    for container in resp.status.container_statuses:
                        if container.name==self.test_container_name and container.state.terminated is not None:
                            if container.state.terminated.reason=='Completed':
                                SUCCESS = True
                            test_container_status=True
                            break
                    if test_container_status:
                        break
                time.sleep(5)
                runtime -= 5
        except Exception,e:
            print str(e)
            return self.client.ERROR
        finally:
            try:
                print(self.kubernetes_api.get_pod_log(self.pod_name, container=self.test_container_name))
                time.sleep(5)
            except Exception as e:
                print(str(e))

        if SUCCESS:
            return self.client.SUCCESS
        return self.client.FAILURE

    def _setup(self):
        self._state = {
            'configmap': {},
            'pod': {}
        }
        self.kubernetes_api = KubernetesAPI()
        self.test_container_name = self.pod_template['metadata']['name']
        self.pod_name = self.test_container_name+'-pod'
        self.pod_template['metadata']['name']=self.pod_name
        self.config_maps = {}
        self.parser_results_directory = self.test_results_directory
        self.listener_results_dir = '/var/opereto_listener_results'


    def _teardown(self):
        self.kubernetes_api = KubernetesAPI()
        current_state = self._get_state()
        if not self.input['keep_pod_running']:
            if self.output_file_path:
                try:
                    self.kubernetes_api.cp(self.pod_name, self.output_file_path, self.task_output_json)
                except Exception as e:
                    print('Failed to process output data from output files {}: {}'.format(self.output_file_path, e))

            if current_state['pod']:
                try:
                    pod_name = current_state['pod'].keys()[0]
                    self._print_step_title('Deleting worker pod {}..'.format(pod_name))
                    print(self.kubernetes_api.delete_pod(pod_name))
                except Exception as e:
                    print('Failed to remove worker pod {}. Please remove it manually : {}'.format(pod_name, str(e)))
                    self.task_exitcode=1

            if current_state['configmap']:
                for config_name in current_state['configmap'].keys():
                    try:
                        self._print_step_title('Deleting config map {}'.format(config_name))
                        print(self.kubernetes_api.delete_config_map(config_name))
                    except Exception as e:
                        print('Failed to remove config map {}. Please remove it manually : {}'.format(config_name, str(e)))
                        self.task_exitcode = 1

if __name__ == "__main__":
    exit(ServiceRunner().run())
