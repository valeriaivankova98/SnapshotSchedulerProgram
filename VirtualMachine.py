import json
import paramiko
class VirtualMachine:
    def _get_vm(self, vmid): #находит VM в кластере по айди
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.node['hostname'], username=self.node['username'], password=self.node['password'])
            stdout = client.exec_command('pvesh get /cluster/resources -type vm --output-format json')[1]
            json_data = stdout.read().decode('ascii').strip("\n")
        resources = json.loads(json_data)
        return [vm for vm in resources if vm['vmid'] == int(vmid)]
    def __init__(self, node, vmid, test_type, test_attribute):
        self.node = {
            'hostname': node['hostname'],
            'username': node['username'],
            'password': node['password']
        }
        vms = self._get_vm(vmid)
        if vms:
            self.vmid = vmid
            self.node['name'] = vms[0]['node']
            self.name = vms[0]['name']
            self.test_type = test_type
            self.test_attr = test_attribute
            self.is_available = True
        else:
            self.is_available = False
