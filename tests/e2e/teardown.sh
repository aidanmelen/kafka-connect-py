kubectl delete -f confluent-platform.yaml
helm --namespace confluent delete confluent-operator
kubectl delete namespace confluent
pkill -f "port-forward"