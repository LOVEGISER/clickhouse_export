1.对于大规模计算，首选：https://github.com/housepower/spark-clickhouse-connector。由于CK版本较低，因此该方案不适合。
上述方案支持到ClickHouse 21.1.2.15及以上版本，因为该版本CK支持了grpc协议，而spark-clickhouse-connector使用了该grpc作为数据的传输层
2. ClickHouse JDBC ：这种方式会一次将CK的表加载到Spark，对于上TB的表，该方案不适合
3. flink 慢
4. ClickHouse-client任务状态不好跟踪
5. File Engine
6. https://github.com/AlexAkulov/clickhouse-backup
7. https://github.com/blynkkk/clickhouse4j