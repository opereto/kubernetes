from opereto.helpers.services import ServiceTemplate
from kubernetes_api import KubernetesAPI
from opereto.utils.validations import JsonSchemeValidator, included_services_scheme, validate_dict, default_variable_pattern, default_variable_name_scheme, item_properties_scheme
from opereto.exceptions import OperetoRuntimeError
import json

from pyopereto.client import OperetoClient, OperetoClientError
import time


class ServiceRunner(ServiceTemplate):

    def __init__(self, **kwargs):
        self.client = OperetoClient()
        ServiceTemplate.__init__(self, **kwargs)

    def validate_input(self):

        input_scheme = {
            "type": "object",
            "properties": {
                "pod_operation": {
                    "enum": ['create_pod','delete_pod']
                },
                "pod_name": {
                    "type": ["null", "string"]
                },
                "pod_template": {
                    "type": "object"
                },
                "agent_java_config": {
                    "type": "string",
                    "minLength": 1
                },
                "agent_log_level": {
                    "enum": ['info', 'warn', 'error', 'fatal','debug']
                },
                "worker_config": {
                    "type": "string",
                    "minLength": 1
                },
                "agent_properties": item_properties_scheme,
                "required": ['pod_operation', 'pod_template',
                             'agent_java_config', 'agent_log_level', 'worker_config',
                             'agent_properties'],
                "additionalProperties": True
            }
        }

        validator = JsonSchemeValidator(self.input, input_scheme)
        validator.validate()

        if self.input['pod_name'].startswith('opereto-worker-node'):
            raise OperetoRuntimeError(error='Pod name is invalid, this name is used for Opereto standard elastic workers. Please select a different name.')

        ## post_operations
        if self.input['post_operations']:
            validator = JsonSchemeValidator(self.input['post_operations'], included_services_scheme)
            validator.validate()


    def process(self):

        def _modify_agent(agent_id):
            try:
                self.client.get_agent(agent_id)
            except OperetoClientError:
                self.client.create_agent(agent_id=agent_id, name=agent_id,
                                         description='This agent worker is part of {} worker stateful set.'.format(
                                             self.pod_name))
                time.sleep(2)
            agent_properties = self.input['agent_properties']
            agent_properties.update({'opereto.shared': True, 'worker.label': self.pod_name})
            self.client.modify_agent_properties(agent_id, agent_properties)

        def _agents_status(online=True):
            while (True):
                try:
                    agent_attr = self.client.get_agent(self.pod_name)
                    if agent_attr['online']==online:
                        break
                except OperetoClientError:
                    pass
                time.sleep(5)

        def _tearrdown_pod():
            print 'Deleting worker pod..'
            self.pod_info = self.kubernetes_api.delete_pod(self.pod_name)
            print 'Waiting that all worker pods will be offline (may take some time)..'
            _agents_status(online=False)

        if self.pod_operation=='create_pod':
            print 'Creating worker pod..'
            self.kubernetes_api.create_pod(self.pod_template)
            _modify_agent(self.pod_name)
            print 'Waiting that all worker pods will be online (may take some time)..'
            _agents_status(online=True)
            self.pod_info = self.kubernetes_api.get_pod(self.pod_name)
            print self.pod_info.status

            ## run post install services
            for service in self.input['post_operations']:
                input = service.get('input') or {}
                agent = service.get('agents') or self.pod_name
                pid = self.client.create_process(service=service['service'], agent=agent, title=service.get('title'),
                                                 **input)
                if not self.client.is_success(pid):
                    _tearrdown_pod()
                    return self.client.FAILURE

        elif self.pod_operation=='delete_pod':
            _tearrdown_pod()

        else:
            raise OperetoRuntimeError(error='Invalid operation: {}'.format(self.pod_operation))

        return self.client.SUCCESS


    def setup(self):

        self.kubernetes_api = KubernetesAPI()
        self.pod_name = self.input['pod_name']
        self.pod_operation = self.input['pod_operation']
        self.pod_info = {}
        self.pod_template = self.input['pod_template']

        if self.pod_operation=='create_pod':
            if self.pod_name:
                self.pod_template["metadata"]["name"] = self.pod_name
                self.pod_template["spec"]["containers"][0]["name"] = self.pod_name+"-worker"
            else:
                self.pod_name=self.pod_template["metadata"]["name"]

            if not self.pod_template["spec"]["containers"][0].get('env'):
                self.pod_template["spec"]["containers"][0]['env']=[]
            self.pod_template["spec"]["containers"][0]['env'] += [
                {
                    "name": "agent_name",
                    "valueFrom": {
                        "fieldRef": {
                            "fieldPath": "metadata.name"
                        }
                    }
                },
                {
                    "name": "opereto_host",
                    "value": self.input['opereto_host']
                },
                {
                    "name": "opereto_user",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": self.input['worker_config'],
                            "key": "OPERETO_USERNAME"
                        }
                    }
                },
                {
                    "name": "opereto_password",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": self.input['worker_config'],
                            "key": "OPERETO_PASSWORD"
                        }
                    }
                },
                {
                    "name": "javaParams",
                    "value": self.input['agent_java_config'],
                },
                {
                    "name": "log_level",
                    "value": self.input['agent_log_level']
                }
            ]

            print 'Pod template:\n{}'.format(json.dumps(self.pod_template, indent=4))

    def teardown(self):
        print self.pod_info



if __name__ == "__main__":
    exit(ServiceRunner().run())
