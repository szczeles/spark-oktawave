import click
import configparser
import os
import zeep
import requests

def initialize_clients(ctx):
    session = requests.Session()
    session.auth = requests.auth.HTTPBasicAuth(
        'API\{}'.format(ctx.obj['config']['oktawave']['user']), 
        ctx.obj['config']['oktawave']['password'])

    ctx.obj['common_api'] = zeep.Client(
        'https://api.oktawave.com/CommonService.svc?wsdl', 
        transport=zeep.transports.Transport(session=session))

    ctx.obj['client_api'] = zeep.Client(
        'https://api.oktawave.com/ClientsService.svc?wsdl',
        transport=zeep.transports.Transport(session=session))

    logon_data = ctx.obj['common_api'].service.LogonUser(
        user=ctx.obj['config']['oktawave']['user'],
        password=ctx.obj['config']['oktawave']['password'],
        ipAddress='127.0.0.1')['User']
    ctx.obj['client_id'] = logon_data['Client']['ClientId']
    ctx.obj['user_id'] = logon_data['UserId']

def translate_vm_class(ctx, name):
    if not 'vmclass_cache' in ctx.obj:
        dict_type =ctx.obj['common_api'].get_type('ns4:Consts.DAL.Dictionary')
        ctx.obj['vmclass_cache'] = ctx.obj['common_api'].service.GetDictionaryItems(
            dictionary=dict_type('VirtualMachineClass'))

    for vmclass in ctx.obj['vmclass_cache']:
        if vmclass['DictionaryItemNames']['DictionaryItemName'][0]['ItemName'] == name:
            return vmclass['DictionaryItemId']

    raise Exception('Invalid vm class: {}'.format(name))

def get_ssh_key_id(ctx, name):
    if 'ssh_key_id' in ctx.obj:
        return ctx.obj['ssh_key_id']

    ssh_keys = ctx.obj['client_api'].service.GetClientSshKeys(clientId=ctx.obj['client_id'])
    for key in ssh_keys:
        if key['_x003C_Name_x003E_k__BackingField'] == name:
            ctx.obj['ssh_key_id'] = key['_x003C_SshKeyId_x003E_k__BackingField']
            return ctx.obj['ssh_key_id']

    return None

def remove_ssh_key(ctx, name):
    ssh_key_id = get_ssh_key_id(ctx, name)

    ctx.obj['client_api'].service.DeleteSshKeys(
        ids=ctx.obj['client_api'].get_type('ns1:ArrayOfint')(ssh_key_id),
        userLogin=ctx.obj['config']['oktawave']['user'],
        userPassword=ctx.obj['config']['oktawave']['password'],
        clientId=ctx.obj['client_id']
    )

def upload_ssh_key(ctx):
    name = ctx.obj['cluster_name']
    if get_ssh_key_id(ctx, name):
        remove_ssh_key(ctx, name)

    public_key_path = ctx.obj['config']['oktawave']['ssh_key']
    with open(os.path.expanduser(public_key_path)) as f:
        public_key = f.read()

    ctx.obj['client_api'].service.CreateSshKey(
        name=name,
        publicKey=public_key,
        userLogin=ctx.obj['config']['oktawave']['user'],
        userPassword=ctx.obj['config']['oktawave']['password'],
        clientId=ctx.obj['client_id']
    )
    assert get_ssh_key_id(ctx, name) != None

def launch_vm(ctx, name, disk_size, vmclass):
    ctx.obj['client_api'].service.CreateVirtualMachineWithAuthSettings(
        templateId=452, # ubuntu 16.04 LTS
        diskSizeGB=disk_size,
        machineName=name,
        selectedClass=translate_vm_class(ctx, vmclass),
        selectedPaymentMethod=33, # PayAsYouGo, hourly charge
        selectedConnectionType=37, # Unlimited
        clientId=ctx.obj['client_id'],
        vAppType='Machine',
        autoScalingTypeId=187, # Off
        instancesCount=1,
        ipId=0, # auto
        authSettings=ctx.obj['client_api'].get_type('ns4:OciAuthorizationSettings')(
            1398, # ssh key login
            ctx.obj['client_api'].get_type('ns1:ArrayOfint')([ctx.obj['ssh_key_id']])
        )
	)

@click.group()
@click.option('--credentials', help='Path to credentials file', default='~/.spark-oktawave-credentials')
@click.pass_context
def cli(ctx, credentials):
    ctx.obj['config'] = configparser.RawConfigParser()
    ctx.obj['config'].read(os.path.expanduser(credentials))

    initialize_clients(ctx)

@cli.command()
@click.pass_context
def balance(ctx):
    result = ctx.obj['client_api'].service.GetClientBalance(
        clientId=ctx.obj['client_id'], userId=ctx.obj['user_id'])
    click.echo("Client balance: {}".format(result['BalanceInCurrency']))

@cli.command()
@click.argument('cluster-name')
@click.option('--slaves', default=2, help='number of slaves')
@click.option('--disk-size', default=10, help='disk size [GB]')
@click.option('--master-class', default='v1.standard-2.2', help='master class')
@click.option('--slave-class', default='v1.standard-2.2', help='slave class')
@click.pass_context
def launch(ctx, cluster_name, slaves, disk_size, master_class, slave_class):
    ctx.obj['cluster_name'] = cluster_name
    upload_ssh_key(ctx)
    launch_vm(ctx, cluster_name+'-master', disk_size, master_class)
    for i in range(slaves):
        launch_vm(ctx, "{}-slave{}".format(cluster_name, i), disk_size, slave_class)

@cli.command()
@click.pass_context
def list(ctx):
	vms = ctx.obj['client_api'].service.GetVirtualMachines(
		searchParams={
			'ClientId': ctx.obj['client_id'], 
			'SearchText': '-master', 
			'PageSize': 100})['_results']
	if vms:
		print(vms['VirtualMachineView'])

@cli.command()
@click.argument('cluster-name')
@click.pass_context
def destroy(ctx, cluster_name):
    print("destroying {}".format(cluster_name))
    vms = ctx.obj['client_api'].service.GetVirtualMachines(
		searchParams={
			'ClientId': ctx.obj['client_id'], 
			'SearchText': cluster_name + "-",
			'PageSize': 1000})['_results']
    if vms:
        for vm in vms['VirtualMachineView']:
            ctx.obj['client_api'].service.DeleteVirtualMachine(
                virtualMachineId=vm['VirtualMachineId'], 
                clientId=ctx.obj['client_id'])

    if get_ssh_key_id(ctx, cluster_name):
        remove_ssh_key(ctx, cluster_name)


def main():
    cli(obj={})

if __name__ == '__main__':
    main()
