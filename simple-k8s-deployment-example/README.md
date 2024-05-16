# Simple k3s deployment example

This sub-project together with the following instructions act as a guidline on how to to get a simple python application pushed to docker-hub and then deployed in a kubernetes environment.

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

5. Create a deployment and service in Kubernetes. Helpful commands for that:
    ```console 
    # Create or Update a resource
    kubectl apply -f deployment.yaml
    kubectl apply -f service.yaml
    # Delete a resource
    kubectl delete deployment <deployment-name>
    kubectl delete service <service-name>
    # Get all resources
    kubectl get all
    # Get logs
    kubectl logs <resource-name>
    ```
