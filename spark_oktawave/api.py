import zeep
import requests
from concurrent.futures import ThreadPoolExecutor

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

        return [{
            'id': vm['VirtualMachineId']
            } for vm in result['VirtualMachineView']]

    def delete_vm(self, id):
        self.clients_api.service.DeleteVirtualMachine(virtualMachineId=id, clientId=self.client_id)

class Cluster:

    def __init__(self, api, name):
        self.api = api
        self.name = name
        self.ssh_key_id = None

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
        self.api.launch_vm(vm_name, disk_size, vm_class, self.get_ssh_key_id())

    def launch_slaves(self, how_many, disk_size, vm_class):
        with ThreadPoolExecutor(how_many) as executor:
            for i in range(how_many):
                vm_name = "{}-slave{}".format(self.name, i+1)
                executor.submit(self.api.launch_vm, vm_name, disk_size, vm_class, self.get_ssh_key_id())

    def destroy_vms(self):
        vms = self.api.list_vms(search_text=self.name + '-')
        with ThreadPoolExecutor(len(vms)) as executor:
            for vm in vms:
                executor.submit(self.api.delete_vm, vm['id'])
