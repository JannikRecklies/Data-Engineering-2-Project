# K3s Cluster on OpenStack

The purpose of these instructions is to first create a VM on OpenStack acting as the master node in a k3s cluster. Then, from within this VM  a script is gonna be exectuted in order to start more VM's acting as worker nodes. The k3s cluster is then ready to be used.

### Setting up master node (VM with floating IP)
1. Copying content from cloud-config-master.yml

2. Edit content as needed

3. Launch Instance on snic-cloud with copied/edited configuration

4. Give VM floating IP and access it through ssh (The cloud-init process may take a few minutes)

### Setting up worker nodes for K3s Cluster
1. Generate ssh keys: 
    ```console 
    ssh-keygen -t rsa
    ```
    (Set the file path /home/ubuntu/.ssh/cluster-key Do not set the password, simply press enter twice)

2. Replace values for ssh_authorized_keys, K3S_URL, K3S_TOKEN in file cloud-config-worker.yml. The values can be retrieved by entering the following lines in the terminal:
    ```console 
    sudo cat /home/ubuntu/.ssh/cluster-key.pub
    hostname -I
    sudo cat /var/lib/rancher/k3s/server/node-token
    ```

3. Download the Runtime Configuration (RC) file from the SSC site and set API access password (Goto https://cloud.snic.se/, Left frame, under Services "Set your API
password".). 

4. Set the environment variables by sourcing the RC-file in the client VM:
    ```console 
    source UPPMAX\ 2024_1-4-openrc.sh
    ```

5. Replace values in line 11-16 in start_instance.py as needed and run the following command to start VM's:
    ```console 
    python3 start_instance.py
    ```

6. Wait until workers have been finished initializing (the cloud-init process may take a few minutes) and they are being registered as nodes in the k3s cluster. The progress of the cloud-init can be seen by running the following commands:
    ```console 
    ssh -i /home/ubuntu/.ssh/cluster-key appuser@<Worker-VM-IP>
    sudo cat /var/log/cloud-init-output.log
    ```