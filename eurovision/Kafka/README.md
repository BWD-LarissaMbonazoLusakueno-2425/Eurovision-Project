# Eurovision Voting System

## Start containers
docker-compose up -d

## Activeer Python venv
source eurovision_env/bin/activate

## Run load test
python3 load_test.py

## Commando's
sudo docker-compose up -d
sudo docker ps
python3 -m venv eurovision_env
source eurovision_env/bin/activate
pip install -r requirements.txt
python3 load_test.py
python3 test_vote.py