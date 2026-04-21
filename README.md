# MLOps Task

## Run Locally
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Docker

### Build
docker build -t mlops-task .

### Run
docker run --rm mlops-task

## Output
- metrics.json → contains signal rate and latency
- run.log → contains logs