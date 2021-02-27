package com.codejiwei.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @ClassName UserJumpApp
 * @Author codejiwei
 * @Date 2021/2/4 20:56
 * @Version 1.0
 * @Description TODO:
 **/
public class UserJumpApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 设置并行度
        env.setParallelism(4);
//        //1.2 开启checkpoint
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //1.3 设置状态后端
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "userJumpApp"));
//
//        //1.4 设置不自动重启
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        //1.5 设置HDFS登录用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2 从kafka中获取数据
        String sourceTopic = "dwd_page_log";
        String groupID = "user_jump_detail_group";
        DataStreamSource<String> kafkaSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupID));

//        DataStream<String> dataStream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":150000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":300000} "
//                );



        //TODO 3 转换结构 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.map(jsonStr -> JSONObject.parseObject(jsonStr));

//        jsonObjDS.print(">>>>>>>>>>>>>>>>>>>>>>>>>");

        //TODO 4 设置事件时间语义， Flink1.12之前需要使用如下语句设置，1.12开始默认就是事件时间语义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //4.1 指定事件时间的事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        })
        );
        //TODO 5 根据mid分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjWithTSDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 6 配置CEP表达式
        // 思路是：首次访问 + 之后访问其他页面 + 二者时间间隔在10s内，排除这样的情况，就是跳出数据
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //模式1：首次访问页面
                        String lastPageID = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageID != null && lastPageID.length() > 0) {
                            return false;
                        }
                        return true;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //模式2：之后访问其他页面
                        String pageID = jsonObj.getJSONObject("page").getString("page_id");
                        if (pageID != null && pageID.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.milliseconds(10000));

        //TODO 7 应用规则，根据规则筛选流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, pattern);

        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };

        //TODO 8 从筛选之后的流中，提取数据，将超时的数据 放到侧输出流中
//        patternStream.select(new PatternSelectFunction<JSONObject, String>() {
//            @Override
//            public String select(Map<String, List<JSONObject>> map) throws Exception {
//                return null;
//            }
//        })
        /*
        * select 和 flatSelect的区别：
        * - select 往后输出的是以值的方式输出的
        * - flatSelect 往后输出的是以采集器的方式输出的
        *
        * - flatSelect 可以和侧输出流一起使用！
        * - flatSelect 含有满足条件的数据，还有不满足条件的数据(Timeout)
        * */
        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(
                timeoutTag,
                //处理超时的数据：就是我们想要的超时的数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> collector) throws Exception {
                        //从不满足上面规则的数据中：获取所有符合first的json对象，也就是从这个页面数据跳出的！！！
                        List<JSONObject> jsonObjList = pattern.get("first");
                        for (JSONObject jsonObject : jsonObjList) {
                            collector.collect(jsonObject.toString());
                        }
                    }
                },
                //处理没有超时的数据
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> collector) throws Exception {
                        //没有超时的数据，不是跳出统计的范围内
                    }
                });
        //TODO 9 从侧输出流输出 跳出数据
        DataStream<String> userJumpDS = filterDS.getSideOutput(timeoutTag);
        userJumpDS.print(">>>>>>>>>>>>>>");

        //TODO 10 输出到kafka的dwm层
        String sinkTopic = "dwm_user_jump_detail";
        userJumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
