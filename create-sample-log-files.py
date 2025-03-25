import random
from datetime import datetime, timedelta
 
# Sample data pools
ips = ['192.168.1.1', '10.0.0.2', '172.16.0.3', '203.0.113.5', '198.51.100.7']
users = ['-', 'alice', 'bob', 'carol', 'dave', 'eve']
methods = ['GET', 'POST', 'DELETE', 'PUT']
resources = ['/index.html', '/api/data', '/login', '/logout', '/images/logo.png', '/dashboard', '/api/user/123', '/api/order']
protocols = ['HTTP/1.0', 'HTTP/1.1', 'HTTP/2']
status_codes = [200, 201, 302, 404, 500]
sizes = [128, 256, 512, 1024, 2048, 4096, 8192]
 
with open('web_log.log', 'w') as f:
    for _ in range(50):
        ip = random.choice(ips)
        user = random.choice(users)
        # Generate a random timestamp within the last 7 days
        timestamp = (datetime.now() - timedelta(minutes=random.randint(0, 10080))).strftime('%d/%b/%Y:%H:%M:%S +0000')
        method = random.choice(methods)
        resource = random.choice(resources)
        protocol = random.choice(protocols)
        status = random.choice(status_codes)
        size = random.choice(sizes)
 
        log = f'{ip} - {user} [{timestamp}] "{method} {resource} {protocol}" {status} {size}\n'
        f.write(log)