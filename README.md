# cdip-routing

### Create python virtual environment with python 3.7.8
```bash
python3 -m venv <venv>
```

### Activate virtual environment
```bash
. <venv>/bin/activate
```

### Install requirements
```bash
Pip install -r requirements.txt
```

### Prerequisites
* Create .env using .env-example as template
* Set key cloak client and secret
** ensure client is configured as global admin
* Set path to  GCP credentials file (need service account and key)
* Ensure REDIS instance is running and configurations set correctly
* Ensure Portal API instance is running and configurations set correctly

### Running Google PubSub Subscribers
```bash
PYTHONPATH=$(pwd)  python3 subscribers/streaming_subscriber.py
PYTHONPATH=$(pwd)  python3 subscribers/streaming_transformed_subscriber.py
```

### Running Google PubSub Transform Service
```bash
uvicorn transform_service.main:app --port=8200 --reload
```

### Running Kafka Subscribers
```bash
faust -A subscribers.kafka_subscriber worker -l info
```

