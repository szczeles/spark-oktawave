import click
import configparser
import os
import zeep
import time
import subprocess
import requests
import random
import datetime
import string
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

commands = {
    'master': "nohup bash -c 'SPARK_DAEMON_MEMORY=128m apt update && apt install -y openjdk-8-jre-headless ca-certificates-java python3-pip supervisor && wget -qO- http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar xz && mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark && /usr/local/spark/sbin/start-master.sh && pip3 install jupyter && /etc/init.d/supervisor restart' > /var/log/master.log 2>&1 < /dev/null &",
    'slave': "nohup bash -c 'apt update && apt install -y openjdk-8-jre-headless ca-certificates-java && wget -qO- http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar xz && mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark && /usr/local/spark/sbin/start-slave.sh {master_ip}:7077' > /var/log/slave.log 2>&1 < /dev/null &",
    'jupyter-pass': "ps -ef | grep jupyter-notebook | grep -v grep | sed -e's/.*token=\([^ ]\+\).*/\\1/'"
}

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
    task = ctx.obj['client_api'].service.CreateVirtualMachineWithAuthSettings(
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
    print("Launching {} in task {}".format(name, task['AsynchronousOperationId']))

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
@click.option('--slave-class', default='v1.highcpu-4.2', help='slave class')
@click.pass_context
def launch(ctx, cluster_name, slaves, disk_size, master_class, slave_class):
    ctx.obj['cluster_name'] = cluster_name
    upload_ssh_key(ctx)
    launch_vm(ctx, cluster_name+'-master', disk_size, master_class)
    for i in range(slaves):
        launch_vm(ctx, "{}-slave{}".format(cluster_name, i+1), disk_size, slave_class)

    print('Waiting for cloud resources...')
    setup(ctx)

'''
def is_cluster_up(ctx):
    operations = ctx.obj['common_api'].service.GetRunningOperations(
        clientId=ctx.obj['client_id'])
    if not operations:
        return True

    for operation in operations:
        if operation['ObjectName'].startswith(ctx.obj['cluster_name']):
            return False

    return True
'''

def get_running_deployments(ctx):
    operations = ctx.obj['common_api'].service.GetRunningOperations(
        clientId=ctx.obj['client_id'])
    if not operations:
        return set()

    return set([operation['ObjectName'] for operation in operations 
                if operation['ObjectName'].startswith(ctx.obj['cluster_name'])])
    
def get_ip(ctx, server):
    vminfo = (ctx.obj['client_api'].service.GetVirtualMachines(
        searchParams={'ClientId': ctx.obj['client_id'], 'SearchText': server, 'PageSize': 1})
        ['_results'])
    if vminfo:
        return vminfo['VirtualMachineView'][0]['TopAddress']

    raise Exception("Unable to get ip for server {}".format(server))

def run_via_ssh(command, ip, input=None):
    cmd = ['ssh', '-q', '-i', '~/.ssh/id_rsa', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null'] #todo rsa key
    cmd.append('root@{}'.format(ip))
    cmd.append('LC_ALL=en_US.UTF-8 ' + command)
    try:
        output = subprocess.check_output(cmd, input=input.encode('utf-8') if input else None, stderr=subprocess.STDOUT)
        return output.decode('utf-8').strip('\n')
    except subprocess.CalledProcessError as e:
        print("Command {} failed with code {}".format(cmd, e.returncode))
        print(e.output)

def get_master_ip(ctx):
    return get_ip(ctx, '{}-master'.format(ctx.obj['cluster_name']))

# thanks to http://stackoverflow.com/a/2257449/7098262
def generate_password(size):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(size))

def initialize_server(ctx, server):
    ip = get_ip(ctx, server)
    mode = 'master' if server.endswith('-master') else 'slave'
    wait_for_port(ip, 22)

    if mode == 'master':
        scp_template(ip, 'spark-defaults.conf', {
            'master_ip': ip,
            'ocs_tenant': ctx.obj['config']['ocs']['tenant'],
            'ocs_username': ctx.obj['config']['ocs']['user'],
            'ocs_password': ctx.obj['config']['ocs']['password'],
            'ocs_container': ctx.obj['config']['ocs']['container']
        }, '/etc/spark/spark-defaults.conf')

        scp_template(ip, 'jupyter.conf', {
            'jupyter_token': generate_password(8)
        }, '/etc/supervisor/conf.d/jupyter.conf')
        
        scp_template(ip, 'Spark.ipynb', {}, '/root/Spark.ipynb')

    command = commands[mode].format(master_ip=get_master_ip(ctx))
    run_via_ssh(command, get_ip(ctx, server))

def setup(ctx):
    running_deployments = get_running_deployments(ctx)
    while len(running_deployments) > 0:
        time.sleep(10)
        print('.', end='', flush=True)
        current = get_running_deployments(ctx)
        for server in (running_deployments - current):
            initialize_server(ctx, server)
            print('+', end='', flush=True)
        running_deployments = current

    # now spark is initialized
    print()
    print("DONE! MasterUI: http://{master_ip}:8080/, Jupyter: http://{master_ip}:8888/".format(master_ip=get_master_ip(ctx)))

@cli.command()
@click.pass_context
def list(ctx):
    masters = ctx.obj['client_api'].service.GetVirtualMachines(
        searchParams={
            'ClientId': ctx.obj['client_id'], 
            'SearchText': '-master',
            'PageSize': 1000})['_results']

    clusters = [master['VirtualMachineName'].split('-master')[0] for master in masters['VirtualMachineView']] if masters else []
    for cluster in sorted(clusters):
        ctx.invoke(info, cluster_name=cluster, verbose=False)
        print()

def get_price_per_hour(ctx, instance_type):
    if not 'pricelists' in ctx.obj:
        ctx.obj['pricelists'] = {
            pl['VirtualMachineClass']['DictionaryItemId']: pl['PricePerHour'] 
            for pl in ctx.obj['common_api'].service.GetVirtualMachineClassConfigurationsWithPrice(clientId=ctx.obj['client_id'])
        }
    return ctx.obj['pricelists'][instance_type]

def scp_template(ip, filename, variables, destpath):
    with open(os.path.join(BASE_DIR, '..', 'templates', filename)) as f:
        template = string.Template(f.read())
        config = template.substitute(variables)
        run_via_ssh(
            'mkdir -p {} && cat - > {}'.format(
                os.path.dirname(destpath),
                destpath
            ), ip, input=config)

@cli.command()
@click.argument('cluster-name')
@click.pass_context
@click.option('-v', '--verbose', is_flag=True)
def info(ctx, cluster_name, verbose):
    ctx.obj['cluster_name'] = cluster_name
    print("Cluster name: {}".format(cluster_name))
    master_ip = get_master_ip(ctx)
    jupyter_password = run_via_ssh(commands['jupyter-pass'], master_ip)
    if not jupyter_password:
        print("Cluster is initilizing... Try again")
        return

    print("Spark Master UI: http://{}:8080/".format(master_ip))
    print("Jupyter: http://{}:8888/".format(master_ip))
    print("Jupyter password: {}".format(jupyter_password))
    vms = (ctx.obj['client_api'].service.GetVirtualMachines(
        searchParams={ 
            'ClientId': ctx.obj['client_id'], 
            'SearchText': cluster_name + '-',
            'PageSize': 1000})
        ['_results']['VirtualMachineView'])
    total_price = sum([get_price_per_hour(ctx, vm['VMClass']['DictionaryItemId']) for vm in vms])
    print("Price per hour: {:.2f} PLN".format(total_price))
    started_at = min(map(lambda vm: vm['CreationDate'], vms)).replace(tzinfo=datetime.timezone.utc)
    running_seconds = (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds()
    hours, remainder = divmod(running_seconds, 3600)
    print("Running for {} h {} m".format(int(hours), int(remainder/60)))
    print("Slaves: {}".format(len(vms)-1))
    if verbose:
        for vm in vms:
            if vm['VirtualMachineName'] != '{}-master'.format(cluster_name):
                print(' * {}'.format(vm['TopAddress']))


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
    
    def shutdown_vm(id):
        ctx.obj['client_api'].service.DeleteVirtualMachine(
            virtualMachineId=id, 
            clientId=ctx.obj['client_id'])

    if vms:
        with ThreadPoolExecutor(len(vms['VirtualMachineView'])) as executor:
            for vm in vms['VirtualMachineView']:
                executor.submit(shutdown_vm, vm['VirtualMachineId'])

    if get_ssh_key_id(ctx, cluster_name):
        remove_ssh_key(ctx, cluster_name)

def main():
    cli(obj={})

def is_port_open(ip, port):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except:
        return False

def wait_for_port(ip, port):
    while not is_port_open(ip, port):
        time.sleep(1)

if __name__ == '__main__':
    main()
