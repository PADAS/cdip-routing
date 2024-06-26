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
  * ensure client is configured as global admin
* Ensure REDIS instance is running and configurations set correctly
* Ensure Portal API instance is running and configurations set correctly
* For Google Pub Sub
  * Set path to  GCP credentials file (need service account and key)
* For Kafka
  * Ensure Kafka Broker running and configurations set correctly

### Running Google PubSub Subscribers
```bash
PYTHONPATH=$(pwd)  python3 app/subscribers/streaming_subscriber.py
PYTHONPATH=$(pwd)  python3 app/subscribers/streaming_transformed_subscriber.py
```

### Running Google PubSub Transform Service
```bash
uvicorn app.transform_service.main:app --port=8200 --reload
```

### Running Kafka Subscribers
```bash
faust -A app.subscribers.kafka_subscriber worker --without-web -l info
```

