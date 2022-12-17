kubectl create namespace confluent
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace confluent 
kubectl apply -f confluent-platform.yaml
kubectl port-forward service/kafka 9092:external --namespace confluent &
kubectl port-forward service/connect 8083:external --namespace confluent &