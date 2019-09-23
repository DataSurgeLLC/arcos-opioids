!!! Logstashing GZipped JSON data into Elasticsearch
Follow steps below

1. Run spark to generate the JSON (I used EMR, `spark-submit --class io.jekal.arcosArcosMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar dashboard`)
1. Copy files (I used S3, `aws s3 cp s3://arcos-opioid/opioids/arcos_dashboard/ . --recursive`)
1. Run script to load (down below)

!!! to load data
`logstash --path.data /tmp/arcos -f pipeline/load_arcos.conf`
