import socket, random
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
