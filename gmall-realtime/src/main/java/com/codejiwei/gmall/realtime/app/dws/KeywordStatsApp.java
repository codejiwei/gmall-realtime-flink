package com.codejiwei.gmall.realtime.app.dws;

import com.codejiwei.gmall.realtime.app.func.KeywordUDTF;
import com.codejiwei.gmall.realtime.bean.KeywordStats;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.common.GmallConstant;
import com.codejiwei.gmall.realtime.utils.ClickHouseUtil;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2021/2/26
 * Desc: 搜索关键词主题实现
 **/
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境准备
        //1.1 创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
//        //1.3 开启检查点
//        env.enableCheckpointing(60, CheckpointingMode.EXACTLY_ONCE);
//        //1.4 设置检查点配置
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        //1.5 配置状态后端
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "keyword_stats_app"));
//        //1.6 设置登录HDFS用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        //1.7 设置重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
        //TODO 1.8 创建Table环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3 将数据源定义为动态表
        //3.1 声明主题以及消费者组
        String groupId = "keyword_stats_app_group";
        String pageViewSourceTopic = "dwd_page_log";
        //3.2 建表
        tableEnv.executeSql(
                "CREATE TABLE page_view (" +
                        " common MAP<STRING, STRING>," +
                        " page MAP<STRING, STRING>," +
                        " ts BIGINT," +
                        " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );

        //TODO 4 从动态表中查询数据
        //什么数据是关键词？ page中page_id是good_list并且item不为空的
        Table fullwordTable = tableEnv.sqlQuery(
                "select page['item'] fullword,rowtime " +
                        " from page_view " +
                        " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );

        //TODO 5 使用自定义的UDTF函数 对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery(
                "select keyword, rowtime " +
                        " from " + fullwordTable + ", " +
                        " LATERAL TABLE(ik_analyze(fullword)) as t(keyword)"
        );

        //TODO 6.分组、开窗、聚合
        Table reduceTable = tableEnv.sqlQuery(
                "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                        " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        //TODO 7 将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);
        keywordStatsDS.print(">>>>>>>>>>>>>>>>>>>");

        //TODO 8 写入ClickHouse
        keywordStatsDS.addSink(ClickHouseUtil.getJdbcSink(
                "insert into keyword_stats_2021 (keyword,ct,source,stt,edt,ts)" +
                        "values(?,?,?,?,?,?)"
        ));

        env.execute();
    }
}
