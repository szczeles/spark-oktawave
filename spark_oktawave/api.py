import zeep
import requests
from concurrent.futures import ThreadPoolExecutor
import time
from .utils import *
import datetime

class OktawaveApi:
    COMMON_API_WSDL = 'https://api.oktawave.com/CommonService.svc?wsdl'
    CLIENTS_API_WSDL = 'https://api.oktawave.com/ClientsService.svc?wsdl'

    def __init__(self, username, password):
        session = requests.Session()
        session.auth = requests.auth.HTTPBasicAuth('API\{}'.format(username), password)
        self.common_api = zeep.Client(self.COMMON_API_WSDL, 
                transport=zeep.transports.Transport(session=session))
        self.clients_api = zeep.Client(self.CLIENTS_API_WSDL,
                transport=zeep.transports.Transport(session=session))
        self.username = username
        self.password = password
        self.vm_classes_map = None
        self.vm_prices_map = None
        self.authenticate()

    def authenticate(self):
        logon_data = self.common_api.service.LogonUser(
                user=self.username, 
                password=self.password,
                ipAddress='127.0.0.1')['User']
        self.client_id = logon_data['Client']['ClientId']
        self.user_id = logon_data['UserId']

    def get_balance(self):
        return self.clients_api.service.GetClientBalance(
                clientId=self.client_id, userId=self.user_id) \
                ['BalanceInCurrency']

    def get_ssh_keys(self):
        response = self.clients_api.service.GetClientSshKeys(clientId=self.client_id)
        if not response:
            return []
        return [(key['_x003C_SshKeyId_x003E_k__BackingField'], key['_x003C_Name_x003E_k__BackingField']) for key in response]

    def delete_ssh_key(self, key_id):
        self.clients_api.service.DeleteSshKeys(
            ids=self.clients_api.get_type('ns1:ArrayOfint')(key_id),
            userLogin=self.username,
            userPassword=self.password,
            clientId=self.client_id
        )

    def create_ssh_key(self, name, contents):
        self.clients_api.service.CreateSshKey(
            name=name,
            publicKey=contents,
            userLogin=self.username,
            userPassword=self.password,
            clientId=self.client_id
        )

    def translate_vm_class(self, vmclass):
        if not self.vm_classes_map:
            dict_type = self.common_api.get_type('ns4:Consts.DAL.Dictionary')
            self.vm_classes_map = {entry['DictionaryItemNames']['DictionaryItemName'][0]['ItemName']: entry['DictionaryItemId'] 
                for entry in self.common_api.service.GetDictionaryItems(
                    dictionary=dict_type('VirtualMachineClass'))}

        if not vmclass in self.vm_classes_map:
            raise Exception('Invalid vm class: {}'.format(vmclass))

        return self.vm_classes_map[vmclass]

    def launch_vm(self, name, disk_size, vmclass, ssh_key_id):
        task = self.clients_api.service.CreateVirtualMachineWithAuthSettings(
            templateId=452, # ubuntu 16.04 LTS
            diskSizeGB=disk_size,
            machineName=name,
            selectedClass=self.translate_vm_class(vmclass),
            selectedPaymentMethod=33, # PayAsYouGo, hourly charge
            selectedConnectionType=37, # Unlimited
            clientId=self.client_id,
            vAppType='Machine',
            autoScalingTypeId=187, # Off
            ipId=0, # auto
            authSettings=self.clients_api.get_type('ns4:OciAuthorizationSettings')(
                1398, # ssh key mode
                self.clients_api.get_type('ns1:ArrayOfint')([ssh_key_id])
            )
        )
        print("Launching {} in task {}".format(name, task['AsynchronousOperationId']))

    def list_vms(self, search_text):
        result = self.clients_api.service.GetVirtualMachines(
            searchParams={
                'ClientId': self.client_id,
                'SearchText': search_text,
                'PageSize': 1000})['_results']

        if not result:
            return []

        vm_powered_on = 86
        return [{
            'id': vm['VirtualMachineId'],
            'name': vm['VirtualMachineName'],
            'ip': vm['TopAddress'],
            'class_id': vm['VMClass']['DictionaryItemId'],
            'started_at': vm['CreationDate'],
        } for vm in result['VirtualMachineView'] if vm['StatusDictId'] == vm_powered_on]

    def delete_vm(self, id):
        self.clients_api.service.DeleteVirtualMachine(virtualMachineId=id, clientId=self.client_id)

    def get_running_operations(self):
        operations = self.common_api.service.GetRunningOperations(clientId=self.client_id)
        if not operations:
            return set()

        return set(map(lambda op: op['ObjectName'], operations))


    def get_price_per_hour(self, instance_type):
        if not self.vm_prices_map:
            self.vm_prices_map = {
                pl['VirtualMachineClass']['DictionaryItemId']: pl['PricePerHour']
                for pl in self.common_api.service.GetVirtualMachineClassConfigurationsWithPrice(clientId=self.client_id)
            }
        return self.vm_prices_map[instance_type]

class Cluster:

    def __init__(self, api, name, ssh_key=None):
        self.api = api
        self.name = name
        self.ssh_key_id = None
        self.uninitialized_hosts = set()
        self.vms = {}
        self.ssh_key = ssh_key
        self._refresh_nodes()

    def get_ssh_key_id(self):
        if self.ssh_key_id:
            return self.ssh_key_id

        for key in self.api.get_ssh_keys():
            if key[1] == self.name:
                return key[0]

        return None

    def remove_ssh_key(self):
        if self.get_ssh_key_id() != None:
            self.api.delete_ssh_key(self.get_ssh_key_id())

    def upload_ssh_key(self, path):
        if self.get_ssh_key_id():
            self.remove_ssh_key()

        with open(path) as f:
            public_key = f.read()

        self.api.create_ssh_key(self.name, public_key)

    def launch_master(self, disk_size, vm_class):
        vm_name = "{}-master".format(self.name)
        self.uninitialized_hosts.add(vm_name)
        self.api.launch_vm(vm_name, disk_size, vm_class, self.get_ssh_key_id())

    def launch_slaves(self, how_many, disk_size, vm_class):
        with ThreadPoolExecutor(how_many) as executor:
            for i in range(how_many):
                vm_name = "{}-slave{}".format(self.name, i+1)
                self.uninitialized_hosts.add(vm_name)
                executor.submit(self.api.launch_vm, vm_name, disk_size, vm_class, self.get_ssh_key_id())

    def destroy_vms(self):
        vms = self.api.list_vms(search_text=self.name + '-')
        with ThreadPoolExecutor(len(vms)) as executor:
            for vm in vms:
                executor.submit(self.api.delete_vm, vm['id'])

    def initialize_master(self, ip, ocs_credentials):
        ocs_tenant, ocs_user = ocs_credentials['user'].split(':')
        copy_file(ip, self.ssh_key, 'spark-defaults.conf', {
            'master_ip': ip,
            'ocs_tenant': ocs_tenant,
            'ocs_username': ocs_user,
            'ocs_password': ocs_credentials['password'],
            'ocs_container': ocs_credentials['container']
        }, '/etc/spark/spark-defaults.conf')

        copy_file(ip, self.ssh_key, 'jupyter.conf', {
            'jupyter_token': generate_password(8)
        }, '/etc/supervisor/conf.d/jupyter.conf')

        copy_file(ip, self.ssh_key, 'Spark.ipynb', {}, '/root/Spark.ipynb')
        command = '''
        nohup bash -c 'SPARK_DAEMON_MEMORY=128m apt update && 
        apt install -y openjdk-8-jre-headless ca-certificates-java python3-pip supervisor && 
        wget -qO- http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar xz && 
        mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark && 
        /usr/local/spark/sbin/start-master.sh && 
        pip3 install jupyter && 
        /etc/init.d/supervisor restart' 
        > /var/log/nohup.out 2>&1 < /dev/null &
        '''.replace('\n', '')
        run_command(ip, self.ssh_key, command)

    def initialize_slave(self, ip):
        command = '''
        nohup bash -c 'apt update && 
        apt install -y openjdk-8-jre-headless ca-certificates-java && 
        wget -qO- http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar xz && 
        mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark && 
        /usr/local/spark/sbin/start-slave.sh {master_ip}:7077' 
        > /var/log/nohup.out 2>&1 < /dev/null &
        '''.replace('\n', '').format(master_ip=self.get_master_ip())
        run_command(ip, self.ssh_key, command)

    def _refresh_nodes(self):
        vms = self.api.list_vms(search_text=self.name + '-')
        self.vms = {vm['name']: vm for vm in vms}
            
    def get_host_info(self, host):
        if not host in self.vms:
            self._refresh_nodes()

        return self.vms[host] if host in self.vms else None

    def get_ip(self, host):
        host_info = self.get_host_info(host)
        if not host_info:
            raise Exception("Unable to get IP for host {}".format(host))
        return host_info['ip']


    def get_master_ip(self):
        return self.get_ip('{}-master'.format(self.name))

    def is_master(self, host):
        return host == '{}-master'.format(self.name)
            
    def initialize_host(self, host, ocs_credentials):
        ip = self.get_ip(host)
        if self.is_master(host):
            self.initialize_master(ip, ocs_credentials)
        else:
            self.initialize_slave(ip)

    def initialize(self, ocs_credentials, check_interval=10):
        while len(self.uninitialized_hosts) > 0:
            unprepared_hosts = set(filter(lambda op: op.startswith(self.name), self.api.get_running_operations()))
            ready_vms = self.uninitialized_hosts - unprepared_hosts
            for host in ready_vms:
                self.initialize_host(host, ocs_credentials)
                self.uninitialized_hosts.remove(host)
                print('M' if self.is_master(host) else 'S', end='', flush=True)

            if len(self.uninitialized_hosts):
                print('.', end='', flush=True)
                time.sleep(check_interval)
        print()

    def get_jupyter_token(self):
        command = "ps -ef | grep jupyter-notebook | grep -v grep | sed -e's/.*token=\([^ ]\+\).*/\\1/'"
        return run_command(self.get_master_ip(), self.ssh_key, command)

    def get_nodes(self):
        return self.vms.values()

    def get_hourly_charge(self):
        return sum(map(lambda vm: self.api.get_price_per_hour(vm['class_id']), self.get_nodes()))

    def get_uptime(self):
        started_at = min(map(lambda vm: vm['started_at'], self.get_nodes())).replace(tzinfo=datetime.timezone.utc)
        running_seconds = (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds()
        hours, remainder = divmod(running_seconds, 3600)
        return "{}h {}m".format(int(hours), int(remainder/60))

    def get_node_function(self, host):
        return host.split(self.name + '-')[1]

    def install_collectd(self, graphite_host):
        command = 'apt-get install -o Dpkg::Options::="--force-confold" -y --no-install-recommends collectd'

        def run_on_node(node):
            copy_file(node['ip'], self.ssh_key, 'collectd.conf', {
                'hostname': self.get_node_function(node['name']),
                'cluster_name': self.name,
                'graphite_host': graphite_host
            }, '/etc/collectd/collectd.conf')
            
            run_command(node['ip'], self.ssh_key, command)

        with ThreadPoolExecutor(len(self.get_nodes())) as executor:
            for node in self.get_nodes():
                executor.submit(run_on_node, node)
