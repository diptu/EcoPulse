# Create main directories
mkdir -p app/{config,api/v1/endpoints,core,db/models,migrations,ml,llm,services} \
         tests/{api,ml,llm} \
         notebooks \
         docker

# Create Python files
touch app/main.py
touch app/config/settings.py
touch app/core/{events.py,utils.py,logger.py}
touch app/db/session.py
touch app/db/models/{prediction.py,insight.py}
touch app/ml/{datasets.py,ann.py,cnn.py,lstm.py,transformer.py,deepriver.py,train.py}
touch app/llm/{prompts.py,inference.py}
touch app/services/{prediction_service.py,insight_service.py}
touch api/v1/router.py
touch api/v1/endpoints/{predict.py,insights.py,metrics.py}

# Create root files
touch README.md requirements.txt pyproject.toml .env
touch docker/{Dockerfile,docker-compose.yml}

# Optional: create empty notebooks
touch notebooks/{eda.ipynb,model_training.ipynb,inference_demo.ipynb}
