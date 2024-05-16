# Deployment of the whole analytic system in K8s

## Step 1: Deploy Pulsar Cluster
The following instructions are closely linked to the official guide given by Apache Pulsar (https://pulsar.apache.org/docs/3.2.x/getting-started-helm/) with a few minor changes.

1. Add Pulsar charts repo.
    ```console
    helm repo add apache https://pulsar.apache.org/charts
    ```
    ```console
    helm repo update
    ```

2. Clone the Pulsar Helm chart repository.
    ```console
    git clone https://github.com/apache/pulsar-helm-chart
    ```
    ```console
    cd pulsar-helm-chart
    ```

3. Use the Pulsar Helm chart to install the Pulsar key components to Kubernetes.
    ```console
    helm install --values ../pulsar-helm-values.yaml pulsar apache/pulsar
    ```

4. Check health of all deployed resources:
    ```console
    kubectl get all
    ```


## Setp 2: Deploy processing and analytics applications/services

1. Switch location in terminal to the deployment folder.

2. Create all Kubernetes resources for the processing/analytics:
    ```console 
    kubectl apply -f analytics-resources
    ```

3. Check health of all deployed resources:
    ```console
    kubectl get all
    ```