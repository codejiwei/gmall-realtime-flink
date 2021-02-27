package com.codejiwei.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.bean.VisitorStats;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.ClickHouseUtil;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * Author: codejiwei
 * Date: 2021/2/22
 * Desc: 访客主题宽表计算
 **/
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境准备
        //1.1 创建流式执行环境
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

        //TODO 2 从Kafka中获取数据
        //2.1 Kafka主题和消费者组信息
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        //2.2 从Kafka中获取dwd层 页面信息
        DataStreamSource<String> pageViewJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        //2.3 从Kafka中获取dwm层 独立访问信息
        DataStreamSource<String> uniqueVisitJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        //2.4 从Kafka中获取dwm层 用户跳转信息
        DataStreamSource<String> userJumpDetailJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

//        pageViewJsonStrDS.print("pv>>>>>>>>>>>>>>>>");
//        uniqueVisitJsonStrDS.print("uv>>>>>>>>>>>>>>>>>");
//        userJumpDetailJsonStrDS.print("uj>>>>>>>>>>>>>>>>>>>>>");

        //TODO 3 对读取流进行结构转换
        //3.1 pageView页面信息转换成VisitorStats
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDS = pageViewJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                return new VisitorStats("", "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"), jsonObj.getLong("ts"));
            }
        });
        //3.2 uniqueVisit独立访问信息 转换成VisitorStats
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDS = uniqueVisitJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                return new VisitorStats("", "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
            }
        });
        //3.3 提取进入页面数，从dwd_page_log中提取
        SingleOutputStreamOperator<VisitorStats> sessionVisitStatsDS = pageViewJsonStrDS.process(new ProcessFunction<String, VisitorStats>() {
            //既需要对数据进行结构转换 转换成VisitorStats；又要过滤掉从其他页面跳转进来的
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    VisitorStats visitorStats = new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));

                    //过滤后 向后面输出
                    out.collect(visitorStats);
                }

            }
        });
        //3.4 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpDetailJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                return new VisitorStats("", "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
            }
        });

        //TODO 4 将四条流union起来
        DataStream<VisitorStats> unionDetailDS = pageViewStatsDS.union(uniqueVisitStatsDS, sessionVisitStatsDS, userJumpStatsDS);

        //TODO 5 设置水位线
//        unionDetailDS.print(">>>>>>>>>>>>>>");
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                                return visitorStats.getTs();
                            }
                        })
        );
        //TODO 6 根据四个维度进行分组（版本、渠道、地区、新老用户）
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsKeyedDS = visitorStatsWithWatermarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<>(
                        visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new()
                );
            }
        });

        //TODO 7 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = visitorStatsKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 8 窗口内聚合 以及 补充窗口起始时间字段
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
                                                                              @Override
                                                                              public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                                                                                  stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                                                                                  stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                                                                                  stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                                                                                  stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                                                                                  stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                                                                                  return stats1;
                                                                              }
                                                                          },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : elements) {
                            String startDate = sdf.format(new Date(context.window().getStart()));
                            String endDate = sdf.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        reduceDS.print("reduce>>>>>>>>>>>>");

        //TODO 9 向clickhouse中插入数据

//        reduceDS.addSink(JdbcSink.<VisitorStats>sink(
//                "insert into visitor_stats_2021 value(?,?,?,?,?,?,?,?,?,?,?,?)",
//                new JdbcStatementBuilder<VisitorStats>() {
//                    @Override
//                    public void accept(PreparedStatement preparedStatement, VisitorStats visitorStats) throws SQLException {
//
//                    }
//                },
//                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().build()
//
//        ));

        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        env.execute();
    }
}
