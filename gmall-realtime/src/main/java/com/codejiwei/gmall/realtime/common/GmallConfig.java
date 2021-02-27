package com.codejiwei.gmall.realtime.common;

/**
 * @ClassName GmallConfig
 * @Description TODO 项目配置常量类
 * @Author codejiwei
 * @Date 2021/2/2 12:27
 * @Version 1.0
 **/
public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL_REALTIME_FLINK";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String CHECKPOINT_FILE_HEAD = "hdfs://hadoop102:8020/gmall/flink/checkpoint/";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
}
