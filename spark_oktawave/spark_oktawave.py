import click
import configparser
import os
import time
import subprocess
import datetime
import string
from api import *
from utils import *

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

commands = {
    'master': "nohup bash -c 'SPARK_DAEMON_MEMORY=128m apt update && apt install -y openjdk-8-jre-headless ca-certificates-java python3-pip supervisor && wget -qO- http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar xz && mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark && /usr/local/spark/sbin/start-master.sh && pip3 install jupyter && /etc/init.d/supervisor restart' > /var/log/master.log 2>&1 < /dev/null &",
    'slave': "nohup bash -c 'apt update && apt install -y openjdk-8-jre-headless ca-certificates-java && wget -qO- http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar xz && mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark && /usr/local/spark/sbin/start-slave.sh {master_ip}:7077' > /var/log/slave.log 2>&1 < /dev/null &",
    'jupyter-pass': "ps -ef | grep jupyter-notebook | grep -v grep | sed -e's/.*token=\([^ ]\+\).*/\\1/'"
}

@click.group()
@click.option('--credentials', help='Path to credentials file', default='~/.spark-oktawave-credentials')
@click.pass_context
def cli(ctx, credentials):
    ctx.obj['config'] = configparser.RawConfigParser()
    ctx.obj['config'].read(os.path.expanduser(credentials))
    ctx.obj['api'] = OktawaveApi(ctx.obj['config']['oktawave']['user'], 
            ctx.obj['config']['oktawave']['password'])

@cli.command()
@click.pass_context
def balance(ctx):
    click.echo("Client balance: {}".format(ctx.obj['api'].get_balance()))

@cli.command()
@click.argument('cluster-name')
@click.option('--slaves', default=2, help='number of slaves')
@click.option('--disk-size', default=10, help='disk size [GB]')
@click.option('--master-class', default='v1.standard-2.2', help='master class')
@click.option('--slave-class', default='v1.highcpu-4.2', help='slave class')
@click.pass_context
def launch(ctx, cluster_name, slaves, disk_size, master_class, slave_class):
    cluster = Cluster(ctx.obj['api'], cluster_name)
    cluster.upload_ssh_key(os.path.expanduser(ctx.obj['config']['oktawave']['ssh_key']))
    cluster.launch_master(disk_size, master_class)
    cluster.launch_slaves(slaves, disk_size, slave_class)

    print('Waiting for cloud resources...')
    #setup(ctx)

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
    cluster = Cluster(ctx.obj['api'], cluster_name)
    cluster.destroy_vms()
    cluster.remove_ssh_key()

def main():
    cli(obj={})

if __name__ == '__main__':
    main()


