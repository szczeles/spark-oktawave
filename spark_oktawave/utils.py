import socket, random, string, subprocess, os

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

def is_port_open(ip, port):
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

# thanks to http://stackoverflow.com/a/2257449/7098262
def generate_password(size):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(size))

def run_command(ip, ssh_key, command, input=None):
    wait_for_port(ip, 22)
    cmd = ['ssh', '-q', '-i', ssh_key, '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null'] 
    cmd.append('root@{}'.format(ip))
    cmd.append('LC_ALL=en_US.UTF-8 ' + command)
    try:
        input = input.encode('utf-8') if input else None
        output = subprocess.check_output(cmd, input=input, stderr=subprocess.STDOUT)
        return output.decode('utf-8').strip('\n')
    except subprocess.CalledProcessError as e:
        print("Command {} failed with code {}".format(cmd, e.returncode))
        print(e.output)

def copy_file(ip, ssh_key, template_name, variables, target_file):
    wait_for_port(ip, 22)
    with open(os.path.join(BASE_DIR, 'templates', template_name)) as f:
        template = string.Template(f.read())
        config = template.substitute(variables)
        run_command(ip, ssh_key,
            'mkdir -p {} && cat - > {}'.format(
                os.path.dirname(target_file),
                target_file
            ), input=config)
