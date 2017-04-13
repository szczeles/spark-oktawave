import click

@click.group()
@click.option('--credentials', help='Path to credentials file', default='~/.spark-oktawave-credentials')
def cli(credentials):
    print("loading credentials from {}".format(credentials))

@cli.command()
@click.argument('cluster-name')
def launch(cluster_name):
    print("launching {}".format(cluster_name))

@cli.command()
@click.argument('cluster-name')
def destroy(cluster_name):
    print("destroying {}".format(cluster_name))

def main():
    cli()

if __name__ == '__main__':
    main()
