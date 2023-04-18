# coincap_kafka_questdb
This is a demo of a data pipeline that collects real time ETH data from coincap's API and stores it in questDB through Kafka
原文介绍:https://dzone.com/articles/building-a-data-pipeline-using-questdb-and-conflue
中文介绍：https://www.modb.pro/db/474795

## 将postgres-sink-eth.json 信息注册到kafka
```shell
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" --data @postgres-sink-eth.json http://localhost:8083/connectors
```

## 去9000端口questdb建表
···sql
CREATE TABLE topic_ETH (`timestamp` timestamp, currency symbol, amount float);
···

## 运行producer.py,每15秒获取数据通过 kafka 存入questdb
···python
python -m producer.py
···