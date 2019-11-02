!!! Logstashing GZipped JSON data into Elasticsearch
Follow steps below

1. Run spark to generate the JSON (I used EMR, `spark-submit --class io.jekal.arcosArcosMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar dashboard`)
1. Copy files (I used S3, `aws s3 cp s3://arcos-opioid/opioids/arcos_dashboard/ . --recursive` or `aws s3 cp s3://arcos-opioid/opioids/arcos_dashboard/ /tmp/data --recursive` for aws)
1. Create `arcos` index (down below)
1. Run script to load (down below)

!!! to load data
`logstash --path.data /tmp/arcos -f pipeline/load_arcos.conf`
or
`logstash --path.data /tmp/arcos -f /tmp/pipeline/load_arcos.conf` for aws

!!! to create index

```
PUT arcos
{
  "settings" : {
    "index" : {
      "number_of_shards" : "10",
      "number_of_replicas" : "1"
    }
  },
  "mappings" : {
    "dynamic" : "false",
    "properties" : {
      "BUYER_ADDL_CO_INFO" : {
        "type" : "text"
      },
      "BUYER_ADDRESS1" : {
        "type" : "text"
      },
      "BUYER_BUS_ACT" : {
        "type" : "keyword"
      },
      "BUYER_CITY" : {
        "type" : "keyword"
      },
      "BUYER_COUNTY" : {
        "type" : "keyword"
      },
      "BUYER_DEA_NO" : {
        "type" : "keyword"
      },
      "BUYER_NAME" : {
        "type" : "text"
      },
      "BUYER_STATE" : {
        "type" : "keyword"
      },
      "BUYER_ZIP" : {
        "type" : "keyword"
      },
      "CALC_BASE_WT_IN_GM" : {
        "type" : "float"
      },
      "COUNTY_FIPS" : {
        "type" : "keyword"
      },
      "COUNTY_NAME" : {
        "type" : "keyword"
      },
      "Combined_Labeler_Name" : {
        "type" : "text"
      },
      "DOSAGE_UNIT" : {
        "type" : "float"
      },
      "DRUG_CODE" : {
        "type" : "keyword"
      },
      "DRUG_NAME" : {
        "type" : "keyword"
      },
      "Ingredient_Name" : {
        "type" : "keyword"
      },
      "MME_Conversion_Factor" : {
        "type" : "float"
      },
      "Measure" : {
        "type" : "keyword"
      },
      "NDC_NO" : {
        "type" : "keyword"
      },
      "Product_Name" : {
        "type" : "keyword"
      },
      "QUANTITY" : {
        "type" : "float"
      },
      "Reporter_family" : {
        "type" : "keyword"
      },
      "Revised_Company_Name" : {
        "type" : "keyword"
      },
      "STATE_COUNTY_FIPS" : {
        "type" : "keyword"
      },
      "STATE_FIPS" : {
        "type" : "keyword"
      },
      "TRANSACTION_CODE" : {
        "type" : "keyword"
      },
      "TRANSACTION_DATE" : {
        "type" : "date"
      },
      "TRANSACTION_ID" : {
        "type" : "keyword"
      },
      "dos_str" : {
        "type" : "float"
      },
      "location" : {
        "type" : "geo_point"
      },
      "population" : {
        "type" : "long"
      }
    }
  }
}
```

!!! snapshot and restore using s3 repository
within es01 & es02 containers, `bin/elasticsearch-plugin install repository-s3`
`docker-compose restart`
```
PUT /_snapshot/arcos/snapshot
{
  "indices": "arcos",
  "ignore_unavailable": true,
  "include_global_state": false,
  "metadata": {
    "taken_by": "cjekal",
    "taken_because": "to load index on cloud service"
  }
}
```
check status via `GET /_snapshot/arcos/snapshot`




!!!AWS EC2
Make sure to pick the EMR IAM role so that you don't need to configure AWS keys :)

!!!AWS EC2 Logstash install
!!Logstash install (see https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/amazon-linux-install.html)
`sudo yum install java-11-amazon-corretto-headless`
`java -version` should mention Corretto
`sudo rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch`
add the following to `/etc/yum.repos.d/logstash.repo` using `sudo vi`
```
[logstash-7.x]
name=Elastic repository for 7.x packages
baseurl=https://artifacts.elastic.co/packages/7.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md
```
`sudo yum install logstash`
`cd /usr/share/logstash/`
`bin/logstash --version`

!!Mounting NVMe SSDs (see https://forums.aws.amazon.com/message.jspa?messageID=390415)
`sudo fdisk -l` and get device name e.g. `/dev/nvme1n1`
`sudo mount -l` to check what's already mounted (so we can format and mount the same way) e.g. `xfs`

`sudo mkdir /mnt2`
`sudo mkfs -t xfs /dev/nvme1n1`
`sudo mount -t xfs /dev/nvme1n1 /mnt2`

`sudo mkdir /mnt2/data`
`cd /mnt2`
`sudo chown ec2-user:ec2-user data`
`cd data`
`aws s3 cp s3://arcos-opioid/opioids/arcos_dashboard/ . --recursive`
`cd /mnt2`
`sudo mkdir /mnt2/pipeline`
`sudo chown ec2-user:ec2-user pipeline`
`aws s3 cp s3://arcos-opioid/opioids/load_arcos.conf .`
`sudo mkdir -p /mnt2/arcos`
`sudo chown ec2-user:ec2-user /mnt2/arcos`
`cd /usr/share/logstash`
`sudo bin/logstash-plugin install logstash-output-amazon_es` see https://github.com/awslabs/logstash-output-amazon_es
`bin/logstash --path.data /mnt2/arcos -f /mnt2/pipeline/load_arcos.conf`
