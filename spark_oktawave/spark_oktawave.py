import click
import configparser
import os
from .api import *

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
    cluster = Cluster(ctx.obj['api'], cluster_name, ctx.obj['config']['oktawave']['private_ssh_key'])
    cluster.upload_ssh_key(os.path.expanduser(ctx.obj['config']['oktawave']['public_ssh_key']))
    cluster.launch_master(disk_size, master_class)
    cluster.launch_slaves(slaves, disk_size, slave_class)
    print('Waiting for cluster initialization...')
    cluster.initialize(ctx.obj['config']['ocs'])

@cli.command()
@click.pass_context
def list(ctx):
    master_nodes = ctx.obj['api'].list_vms(search_text='-master')
    clusters = map(lambda vm: vm['name'].split('-master')[0], master_nodes)
    for cluster in sorted(clusters):
        ctx.invoke(info, cluster_name=cluster, verbose=False)
        print()

@cli.command()
@click.argument('cluster-name')
@click.argument('graphite-host')
@click.pass_context
def enable_monitoring(ctx, cluster_name, graphite_host):
    cluster = Cluster(ctx.obj['api'], cluster_name, ctx.obj['config']['oktawave']['private_ssh_key'])
    cluster.install_collectd(graphite_host)

@cli.command()
@click.argument('cluster-name')
@click.pass_context
@click.option('-v', '--verbose', is_flag=True)
def info(ctx, cluster_name, verbose):
    cluster = Cluster(ctx.obj['api'], cluster_name, ctx.obj['config']['oktawave']['private_ssh_key'])

    print("Cluster name: {}".format(cluster.name))
    master_ip = cluster.get_master_ip()
    jupyter_password = cluster.get_jupyter_token()
    if not jupyter_password:
        print("Cluster is initilizing... Try again")
        return

    print("Spark Master UI: http://{}:8080/".format(master_ip))
    print("Jupyter: http://{}:8888/".format(master_ip))
    print("Jupyter password: {}".format(jupyter_password))
    print("Price per hour: {:.2f} PLN".format(cluster.get_hourly_charge()))
    print("Running for {}".format(cluster.get_uptime()))
    print("Slaves: {}".format(len(cluster.get_nodes()) - 1))
    if verbose:
        for node in cluster.get_nodes():
            if not cluster.is_master(node['name']):
                print(' * {}'.format(node['ip']))


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
