package com.codejiwei.gmall.realtime.app.dws;

import com.codejiwei.gmall.realtime.bean.ProvinceStats;
import com.codejiwei.gmall.realtime.utils.ClickHouseUtil;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2021/2/25
 * Desc: 地区主题FlinkSQL实现
 **/
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境准备
        //1.1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
//        //1.3 开启检查点ck
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //1.4 设置检查点配置
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //1.5 设置状态后端
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "visitorStat_app"));
//        //1.6 设置重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        //1.7 设置登录HDFS的用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO 1.8 定义Table流环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2 把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //TODO 3 聚合计算
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code area_code," +
                "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");
        //TODO 4 将动态表转换为数据流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        //这两种转换成数据流的方式区别：toAppendStream的方式是仅通过insert操作
        //RetractStream的方式是包含insert、delete、update操作
//        DataStream<Tuple2<Boolean, ProvinceStats>> provinceStatsDS = tableEnv.toRetractStream(provinceStateTable, ProvinceStats.class);

        provinceStatsDS.print(">>>>>>>>>>>");

        //TODO 5 将流中的数据保存到ClickHouse中
        provinceStatsDS.addSink(ClickHouseUtil.getJdbcSink("insert into province_stats_2021 values (?,?,?,?,?,?,?,?,?,?)"));


        env.execute();
    }

}
