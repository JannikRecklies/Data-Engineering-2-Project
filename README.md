# DataEngineering2

### Openstack-setup: Setting up master node (VM with floating IP)
1. Copying content from cloud-config-master.yml

2. Edit content as needed

3. Launch Instance on snic-cloud with copied/edited configuration

4. Give VM floating IP and access it through ssh (The cloud-init process may take a few minutes)

### Openstack-setup: Setting up worker nodes for K3s Cluster
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

### Simple k3s deployment example

1. Building the Docker Image
    ```console 
    docker build -t <your-dockerhub-username>/test-python-app:latest .

    ```

2. Push the Docker Image to a Container Registry
    ```console 
    docker login
    docker push your-dockerhub-username/your-python-app:latest
    ```

3. Now the docker image can be used to setup a deployment as shown in the ./k3s-example.

4. In case the pods are not starting properly, it might help to try out starting the docker container without kubernetes:
    ```console 
    docker pull jannikrecklies/test-python-app:latest
    docker run -d -p 5100:5100 jannikrecklies/test-python-app:latest

    docker ps
    docker stop <container_id_or_name>
    docker rm <container_id_or_name>
    ```

5. Kubernetes Deployment:
    ```console 
    # Create or Update a Deployment
    kubectl apply -f deployment.yaml
    # Delete deployment
    kubectl delete deployment <deployment-name>
    # Get all resources
    kubectl get all
    # Get logs
    kubectl logs <deployment-name>
    ```
