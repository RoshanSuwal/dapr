./dapr_scheduling \
  --dapr-http-port 3500 \
  --dapr-grpc-port 50001 \
  --app-id ath-8 \
  --app-port 5005 \
  --app-protocol grpc \
  --http_scheduler_enable_scheduling \
  --http_scheduler_policy edf \
  --grpc_scheduler_enable_scheduling \
  --grpc_scheduler_policy edf \
  --log-as-json \
  --grpc_scheduler_worker 5 \
  --http_scheduler_budget_path /home/roshan/workspace/poc/dapr/dag_budget.json