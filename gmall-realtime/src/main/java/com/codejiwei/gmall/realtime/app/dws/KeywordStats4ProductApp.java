package com.codejiwei.gmall.realtime.app.dws;

import com.codejiwei.gmall.realtime.app.func.KeywordProductUDTF;
import com.codejiwei.gmall.realtime.app.func.KeywordUDTF;
import com.codejiwei.gmall.realtime.bean.KeywordStats;
import com.codejiwei.gmall.realtime.utils.ClickHouseUtil;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2021/2/26
 * Desc: 商品行为关键词实现
 **/
public class KeywordStats4ProductApp {
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
        //TODO 1.8 设置FlinkTable环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2 注册自定义函数
        tableEnv.createTemporarySystemFunction("keywordProduct", KeywordProductUDTF.class);
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 3 创建动态表
        //3.1 定义数据源
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic = "dws_product_stats";
        tableEnv.executeSql(
                "create table product_stats (" +
                        " spu_name STRING," +
                        " click_ct BIGINT," +
                        " cart_ct BIGINT," +
                        " order_ct BIGINT," +
                        " stt STRING," +
                        " edt STRING ) " +
                        "WITH ("+ MyKafkaUtil.getKafkaDDL(productStatsSourceTopic, groupId) +")"
        );
        // TODO 4 聚合计数
        Table keywordStatsProduct = tableEnv.sqlQuery(
                "select keyword,ct,source, stt, edt, UNIX_TIMESTAMP()*1000 ts " +
                        " from product_stats," +
                        " LATERAL TABLE(ik_analyze(spu_name)) as t(keyword)," +
                        " LATERAL TABLE(keywordProduct(click_ct, cart_ct, order_ct)) as t2(ct,source)"
        );

        //TODO 5 转换为数据流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsProduct, KeywordStats.class);

        //TODO 6 输出到clickhouse中去
        keywordStatsDS.print(">>>>>>>>>>>>>>>>>");
        keywordStatsDS.addSink(ClickHouseUtil.getJdbcSink(
                "insert into keyword_product_stats_2021 (keyword, ct, source, stt, edt, ts)" +
                        "values(?,?,?,?,?,?)"
        ));

        env.execute();
    }
}
