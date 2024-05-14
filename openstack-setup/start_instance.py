# http://docs.openstack.org/developer/python-novaclient/ref/v2/servers.html
import time, os, sys, random, re
import inspect
from os import environ as env

from  novaclient import client
import keystoneclient.v3.client as ksclient
from keystoneauth1 import loading
from keystoneauth1 import session

flavor = "ssc.small" 
private_net = "UPPMAX 2024/1-4 Internal IPv4 Network"
floating_ip_pool_name = None
floating_ip = None
image_name = "Ubuntu 20.04 - 2023.12.07"
key_name = "jannik-cloud-key"

identifier = random.randint(1000,9999)

loader = loading.get_plugin_loader('password')

auth = loader.load_from_options(auth_url=env['OS_AUTH_URL'],
                                username=env['OS_USERNAME'],
                                password=env['OS_PASSWORD'],
                                project_name=env['OS_PROJECT_NAME'],
                                project_domain_id=env['OS_PROJECT_DOMAIN_ID'],
                                #project_id=env['OS_PROJECT_ID'],
                                user_domain_name=env['OS_USER_DOMAIN_NAME'])

sess = session.Session(auth=auth)
nova = client.Client('2.1', session=sess)
print ("user authorization completed.")

image = nova.glance.find_image(image_name)

flavor = nova.flavors.find(name=flavor)

if private_net != None:
    net = nova.neutron.find_network(private_net)
    nics = [{'net-id': net.id}]
else:
    sys.exit("private-net not defined.")

#print("Path at terminal when executing this file")
# print(os.getcwd() + "/cloud-config-worker.yml")
cfg_file_path =  os.getcwd()+'/cloud-config-worker.yml'
if os.path.isfile(cfg_file_path):
    userdata_worker_1 = open(cfg_file_path)
else:
    sys.exit("cloud-config-worker.yml is not in current working directory")

cfg_file_path =  os.getcwd()+'/cloud-config-worker.yml'
if os.path.isfile(cfg_file_path):
    userdata_worker_2 = open(cfg_file_path)
else:
    sys.exit("cloud-config-worker.yml is not in current working directory")

secgroups = ['default']

print ("Creating instances ... ")
instance_worker_node_1 = nova.servers.create(name="group_15_worker_1", image=image, flavor=flavor, key_name=key_name,userdata=userdata_worker_1, nics=nics,security_groups=secgroups)
instance_worker_node_2 = nova.servers.create(name="group_15_worker_2", image=image, flavor=flavor, key_name=key_name,userdata=userdata_worker_2, nics=nics,security_groups=secgroups)
inst_status_worker_1 = instance_worker_node_1.status
inst_status_worker_2 = instance_worker_node_2.status

print ("waiting for 10 seconds.. ")
time.sleep(10)

while inst_status_worker_1 == 'BUILD' or inst_status_worker_2 == 'BUILD':
    print ("Instance: "+instance_worker_node_1.name+" is in "+inst_status_worker_1+" state, sleeping for 5 seconds more...")
    print ("Instance: "+instance_worker_node_2.name+" is in "+inst_status_worker_2+" state, sleeping for 5 seconds more...")
    time.sleep(5)
    instance_worker_node_1 = nova.servers.get(instance_worker_node_1.id)
    inst_status_worker_1 = instance_worker_node_1.status
    instance_worker_node_2 = nova.servers.get(instance_worker_node_2.id)
    inst_status_worker_2 = instance_worker_node_2.status

ip_address_prod = None
for network in instance_worker_node_1.networks[private_net]:
    if re.match('\d+\.\d+\.\d+\.\d+', network):
        ip_address_prod = network
        break
if ip_address_prod is None:
    raise RuntimeError('No IP address assigned!')

ip_address_dev = None
for network in instance_worker_node_2.networks[private_net]:
    if re.match('\d+\.\d+\.\d+\.\d+', network):
        ip_address_dev = network
        break
if ip_address_dev is None:
    raise RuntimeError('No IP address assigned!')

print ("Instance: "+ instance_worker_node_1.name +" is in " + inst_status_worker_1 + " state" + " ip address: "+ ip_address_prod)
print ("Instance: "+ instance_worker_node_2.name +" is in " + inst_status_worker_2 + " state" + " ip address: "+ ip_address_dev)
