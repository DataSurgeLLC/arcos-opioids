#!/bin/bash

docker-compose exec master rm -f /usr/spark-2.3.1/jars/snappy*
docker-compose exec --index=1 worker rm -f /usr/spark-2.3.1/jars/snappy*
docker-compose exec --index=2 worker rm -f /usr/spark-2.3.1/jars/snappy*
docker-compose exec --index=3 worker rm -f /usr/spark-2.3.1/jars/snappy*
# docker-compose exec --index=4 worker rm -f /usr/spark-2.3.1/jars/snappy*
# docker-compose exec --index=5 worker rm -f /usr/spark-2.3.1/jars/snappy*

docker-compose exec master curl -s -o /usr/spark-2.3.1/jars/snappy-java-1.1.2.6.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.2.6/snappy-java-1.1.2.6.jar
docker-compose exec --index=1 worker curl -s -o /usr/spark-2.3.1/jars/snappy-java-1.1.2.6.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.2.6/snappy-java-1.1.2.6.jar
docker-compose exec --index=2 worker curl -s -o /usr/spark-2.3.1/jars/snappy-java-1.1.2.6.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.2.6/snappy-java-1.1.2.6.jar
docker-compose exec --index=3 worker curl -s -o /usr/spark-2.3.1/jars/snappy-java-1.1.2.6.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.2.6/snappy-java-1.1.2.6.jar
# docker-compose exec --index=4 worker curl -s -o /usr/spark-2.3.1/jars/snappy-java-1.1.2.6.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.2.6/snappy-java-1.1.2.6.jar
# docker-compose exec --index=5 worker curl -s -o /usr/spark-2.3.1/jars/snappy-java-1.1.2.6.jar https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.2.6/snappy-java-1.1.2.6.jar
