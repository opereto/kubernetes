import time
import json

from opereto.helpers.services import ServiceTemplate
from kubernetes_api import KubernetesAPI
from opereto.utils.validations import JsonSchemeValidator, included_services_scheme, validate_dict, default_variable_pattern, default_variable_name_scheme, item_properties_scheme
from opereto.exceptions import OperetoRuntimeError
from pyopereto.client import OperetoClient, OperetoClientError



class ServiceRunner(ServiceTemplate):

    def __init__(self, **kwargs):
        self.client = OperetoClient()
        ServiceTemplate.__init__(self, **kwargs)

    def validate_input(self):

        input_scheme = {
            "type": "object",
            "properties": {
                "deployment_operation": {
                    "enum": ['create_statefulset', 'modify_statefulset', 'delete_statefulset', 'update_worker_image']
                },
                "deployment_name": {
                    "type": ["null", "string"]
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
                "required": ['deployment_operation', 'agent_java_config', 'agent_log_level', 'worker_config','agent_properties'],
                "additionalProperties": True
            }
        }

        validator = JsonSchemeValidator(self.input, input_scheme)
        validator.validate()

        if self.input['deployment_name']=='opereto-worker-node':
            raise OperetoRuntimeError(error='Deployment name is invalid, this name is used for Opereto standard workers. Please insert different name.')

        ## post_operations
        if self.input['post_operations']:
            validator = JsonSchemeValidator(self.input['post_operations'], included_services_scheme)
            validator.validate()


    def process(self):


        def _get_agent_names():
            names=[]
            for count in range(self.worker_replicas):
                names.append(self.deployment_name+'-'+str(count))
            return names

        def _modify_agent(agent_id):
            try:
                self.client.get_agent(agent_id)
            except OperetoClientError:
                self.client.create_agent(agent_id=agent_id, name=agent_id,
                                         description='This agent worker is part of {} worker stateful set.'.format(
                                             self.deployment_name))
                time.sleep(2)
            agent_properties = self.input['agent_properties']
            agent_properties.update({'opereto.shared': True, 'worker.label': self.deployment_name})
            self.client.modify_agent_properties(agent_id, agent_properties)

        def _agents_status(online=True):
            while (True):
                ok = True
                for agent_id in _get_agent_names():
                    try:
                        agent_attr = self.client.get_agent(agent_id)
                        if agent_attr['online']!=online:
                            ok = False
                            break
                    except OperetoClientError:
                        pass
                if ok:
                    break
                time.sleep(5)

        def _tearrdown_statefileset():
            print 'Deleting worker stateful set..'
            self.deployment_info = self.kubernetes_api.delete_stateful_set(self.deployment_name)
            print 'Waiting that all worker pods will be offline (may take some time)..'
            _agents_status(online=False)


        if self.deployment_operation=='create_statefulset':
            print 'Creating worker stateful set..'
            self.deployment_info = self.kubernetes_api.create_stateful_set(self.deployment_template)
            for agent_id in _get_agent_names():
                _modify_agent(agent_id)
            print 'Waiting that all worker pods will be online (may take some time)..'
            _agents_status(online=True)
            self.deployment_info = self.kubernetes_api.get_stateful_set(self.deployment_name)
            print self.deployment_info.status

            ## run post install services
            post_install_pids=[]
            for agent_id in _get_agent_names():
                for service in self.input['post_operations']:
                    input = service.get('input') or {}
                    agent_name = service.get('agents') or agent_id
                    pid = self.client.create_process(service=service['service'], agent=agent_name, title=service.get('title'),
                                                     **input)
                    post_install_pids.append(pid)
            if post_install_pids and not self.client.is_success(post_install_pids):
                _tearrdown_statefileset()
                return self.client.FAILURE

        elif self.deployment_operation=='modify_statefulset':
            print 'Modifying worker stateful set..'
            self.deployment_info = self.kubernetes_api.modify_stateful_set(self.deployment_name, self.deployment_template)
            for agent_id in _get_agent_names():
                _modify_agent(agent_id)
            print 'Waiting that all worker pods will be online (may take some time)..'
            _agents_status(online=True)
            self.deployment_info = self.kubernetes_api.get_stateful_set(self.deployment_name)
            print self.deployment_info.status

        elif self.deployment_operation=='delete_statefulset':
            _tearrdown_statefileset()

        else:
            raise OperetoRuntimeError(error='Invalid operation: {}'.format(self.deployment_operation))


        return self.client.SUCCESS


    def setup(self):
        self.kubernetes_api = KubernetesAPI()
        self.deployment_name = self.input['deployment_name']
        self.deployment_operation = self.input['deployment_operation']
        self.deployment_info = {}
        self.deployment_template = self.input['deployment_template']
        self.worker_replicas = self.deployment_template["spec"]["replicas"]

        if self.deployment_operation in ['create_statefulset', 'modify_statefulset']:
            if self.deployment_name:
                self.deployment_template["metadata"]["name"] = self.deployment_name
                self.deployment_template["spec"]["template"]["spec"]["containers"][0]["name"] = self.deployment_name+"-worker"
                try:
                    self.deployment_template["spec"]["selector"]["matchLabels"]["app"] = self.deployment_name+"-cluster"
                except:
                    pass
                try:
                    self.deployment_template["spec"]["template"]['metadata']["labels"]["app"] = self.deployment_name+"-cluster"
                except:
                    pass

            else:
                self.deployment_name = self.deployment_template["metadata"]["name"]

            if not self.deployment_template["spec"]["template"]["spec"]["containers"][0].get('env'):
                self.deployment_template["spec"]["template"]["spec"]["containers"][0]['env'] = []

            self.deployment_template["spec"]["template"]["spec"]["containers"][0]['env'] += [
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

            print 'Deployment template:\n{}'.format(json.dumps(self.deployment_template, indent=4))


    def teardown(self):
        print self.deployment_info



if __name__ == "__main__":
    exit(ServiceRunner().run())
