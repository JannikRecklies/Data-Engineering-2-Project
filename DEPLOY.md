# Build and Deploy the Languages flow
pushd /home/ubuntu/Data-Engineering-2-Project/services/gh-api_0
sudo docker build -t jannikrecklies/gh-metadata-fetcher:latest .
sudo docker push jannikrecklies/gh-metadata-fetcher:latest
popd
pushd /home/ubuntu/Data-Engineering-2-Project/services/gh-langs-fetcher_1
sudo docker build -t jannikrecklies/gh-langs-fetcher:latest .
sudo docker push jannikrecklies/gh-langs-fetcher:latest
popd
pushd /home/ubuntu/Data-Engineering-2-Project/services/gh-tdd-devops-fetcher_2
sudo docker build -t jannikrecklies/gh-tdd-devops-processor:latest .
sudo docker push jannikrecklies/gh-tdd-devops-processor:latest
popd
pushd /home/ubuntu/Data-Engineering-2-Project/services/gh-repo-persister_3
sudo docker build -t jannikrecklies/gh-langs-persister:latest .
sudo docker push jannikrecklies/gh-langs-persister:latest
popd
pushd /home/ubuntu/Data-Engineering-2-Project/deployment/analytics-resources
kubectl apply -f ./


# To clean the languages table
kubectl exec -it mysql-75f54cf9ff-dxvtl  -- mysql -uroot -ppassword -e "USE gh-processing-results; DELETE FROM languages;"

# To get top 10 languages
kubectl exec -it mysql-75f54cf9ff-dxvtl  -- mysql -uroot -ppassword -e "USE gh-processing-results; SELECT * FROM languages ORDER BY popularity_count DESC LIMIT 10;"

# To get top 10 tdd languages
kubectl exec -it mysql-75f54cf9ff-dxvtl  -- mysql -uroot -ppassword -e "USE gh-processing-results; SELECT * FROM languages ORDER BY tdd_count DESC LIMIT 10;"

# To get top 10 tdd_and_devops languages
kubectl exec -it mysql-75f54cf9ff-dxvtl  -- mysql -uroot -ppassword -e "USE gh-processing-results; SELECT * FROM languages ORDER BY tdd_and_devops_count DESC LIMIT 10;"

# Build and deploy the commits flow
pushd /home/ubuntu/Data-Engineering-2-Project/services/gh-commit-processor_1
sudo docker build -t jannikrecklies/gh-commits-aggregator:latest .
sudo docker push jannikrecklies/gh-commits-aggregator:latest
popd

pushd /home/ubuntu/Data-Engineering-2-Project/services/gh-repo-commits-persister_3
sudo docker build -t jannikrecklies/gh-commits-persister:latest .
sudo docker push jannikrecklies/gh-commits-persister:latest
popd

pushd /home/ubuntu/Data-Engineering-2-Project/deployment/commits-deployments
kubectl apply -f ./