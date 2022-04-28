# dbt-streaming-kafka

## How to run the project

1. Clone the repository
2. Install the dependencies - python3 dependencies, spark, kafka, postgresql
3. Run Zookeeper
    ```bash
    $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
    ```
4. Run Kafka
    ```bash
    $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
    ``` 

    Note - You **do not need** to create the topics. They will be created automatically on python
5. Visit the psql shell and change the password of the default postgres user to postgres
    ```bash
    ALTER USER postgres WITH PASSWORD 'postgres';
    ```
6. On separate terminals, run each of the following commands:
    ```bash
    python3 streamer.py
    ```
    ```bash
    $SPARK_HOME/bin/spark-submit producer.py > output.log
    ```
    ```bash
    python3 consumer.py
    ```
7. Download Grafana and launch it. Add the PostgreSQL data source to it.
8. Make a panel and add the following configuration:
    - Query:
        ```sql
        select t.start_time as "time", t.hashtag, t.count from twitter t
        where $__timeFilter(t.start_time)
        group by t.start_time, t.hashtag, t.count
        order by t.start_time
        ```
    - Visuzliation Type: Graph(old)
    - Data Source: PostgreSQL
    - Query format as: Time series