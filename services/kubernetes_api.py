import time
from kubernetes import client as kubernetes_client, config as kubernetes_config
from opereto.utils.shell import run_shell_cmd
from opereto.exceptions import OperetoRuntimeError

class KubernetesAPI(object):

    def __init__(self, namespace='default'):
        self.client = kubernetes_client
        self.namespace = namespace
        kubernetes_config.load_incluster_config()
        self.v1 = kubernetes_client.CoreV1Api()
        self.v1b1 = kubernetes_client.AppsV1beta1Api()
        self.v1b2 = kubernetes_client.AppsV1beta2Api()
        self.v1b1Storage = kubernetes_client.StorageV1beta1Api()
        self.v1b1Extensions = kubernetes_client.ExtensionsV1beta1Api()
        self.AppsV1Api = kubernetes_client.AppsV1Api()
        self.v1_body_delete = kubernetes_client.V1DeleteOptions()
        self.batch_api = kubernetes_client.BatchV1Api()

    def get_pods(self):
        res = self.v1.list_namespaced_pod(self.namespace)
        return [i.metadata.name for i in res.items]

    def create_stateful_set(self, deployment_manifest):
        resp = self.AppsV1Api.create_namespaced_stateful_set(
            body=deployment_manifest, namespace=self.namespace)
        return resp

    def modify_stateful_set(self, name, deployment_manifest):
        resp = self.AppsV1Api.patch_namespaced_stateful_set(
            name=name, body=deployment_manifest, namespace=self.namespace)
        return resp

    def delete_stateful_set(self, name):
        resp = self.AppsV1Api.delete_namespaced_stateful_set(name=name, namespace=self.namespace, body={}, grace_period_seconds=0)
        return resp

    def get_stateful_set(self, name):
        resp = self.AppsV1Api.read_namespaced_stateful_set(name=name, namespace=self.namespace)
        return resp

    def create_pod(self, pod_manifest):
        pod_name = pod_manifest['metadata']['name']
        self.v1.create_namespaced_pod(body=pod_manifest, namespace=self.namespace)
        while True:
            resp = self.get_pod(pod_name)
            if resp.status.phase != 'Pending':
                print 'Pod status: {}'.format(resp.status.phase)
                break
            time.sleep(2)
        return resp

    def modify_pod(self, pod_name, deployment_manifest):
        self.v1.patch_namespaced_pod(name=pod_name, body=deployment_manifest, namespace=self.namespace)
        time.sleep(5)
        while True:
            resp = self.get_pod(pod_name)
            if resp.status.phase == 'Running':
                print('Pod status: {}'.format(resp.status.phase))
                break
            time.sleep(1)
        return resp

    def delete_pod(self, pod_name):
        resp = self.v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace,body={})
        return resp

    def get_pod(self, pod_name):
        resp = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
        return resp

    def get_pod_log(self, pod_name, container=None, tail_lines=9900):
            return self.v1.read_namespaced_pod_log(pod_name, self.namespace, container=container, follow=False, tail_lines=tail_lines, pretty='true')

    def cp(self, pod_id, pod_path, current_path, direction='copy_from'):
        if direction=='copy_from':
            (exc, out,err) = run_shell_cmd('kubectl cp {}/{}:{} {}'.format(self.namespace, pod_id, pod_path, current_path))
            return int(exc)
        elif direction=='copy_to':
            (exc, out, err) = run_shell_cmd('kubectl cp {} {}/{}:{}'.format(current_path, self.namespace, pod_id, pod_path))
            return int(exc)
        else:
            raise OperetoRuntimeError(error='Invalid copy direction.')

    def create_config_map(self, configmap_name, config_data={}):
        try:
            if config_data:
                body = kubernetes_client.V1ConfigMap(data=config_data, metadata=kubernetes_client.V1ObjectMeta(name=configmap_name, namespace=self.namespace))
                api_response = self.v1.create_namespaced_config_map(self.namespace, body=body, pretty=True)
                return api_response
        except Exception as e:
            raise OperetoRuntimeError(error="Failed to create config map {}: {}".format(configmap_name, e))


    def delete_config_map(self, configmap_name):
        try:
            api_response = self.v1.delete_namespaced_config_map(configmap_name, self.namespace, pretty=True, body=kubernetes_client.V1DeleteOptions())
            return api_response
        except Exception as e:
            raise OperetoRuntimeError(error='Failed to delete config map {}: {}'.format(configmap_name, e))





