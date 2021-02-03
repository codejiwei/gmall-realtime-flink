package com.codejiwei.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName BaseLogAppBak
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/2 21:05
 * @Version 1.0
 **/
public class BaseLogAppBak {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";

    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint"));

        //设置登录用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //TODO 1 接收kafka的数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        //TODO 2 转换结构
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {

                return JSONObject.parseObject(value);
            }
        });

        //TODO 3 识别新老访客
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonWithFlag = midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            //TODO 首先声明访问状态
            private ValueState<String> firstVisitState;
            private SimpleDateFormat sdf;

            //TODO 初始化访问状态、日期格式化对象
            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                sdf = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //获取当前日志的标记状态
                String isNew = jsonObj.getJSONObject("common").getString("is_new");

                //获取当前日期的时间戳
                String ts = jsonObj.getString("ts");

                if ("1".equals(isNew)) {
                    //获取保存的状态的值
                    String stateDate = firstVisitState.value();
                    //对当前的日期进行格式化
                    String curDate = sdf.format(new Date(ts));

                    //如果当前状态不为空，并且状态的日期和当前日期不相等，说明是个老访客
                    if (stateDate != null && stateDate.length() != 0) {
                        //判断是否是同一天
                        if (!stateDate.equals(curDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    } else {
                        //如果当前的状态为空，那么就更新为今天的日期
                        firstVisitState.update(curDate);
                    }
                }
                return jsonObj;

            }
        });

        //TODO 4 分流
        //定义启动侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };

        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonWithFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context context, Collector<String> collector) throws Exception {
                //从数据中获取启动日志标记
                JSONObject startJsonObj = jsonObj.getJSONObject("start");

                if (startJsonObj != null && startJsonObj.size() > 0) {
                    //说明是启动日志，输出到启动侧输出流
                    context.output(startTag, jsonObj.toString());
                } else {
                    //不是启动日志， 判断是否是曝光日志
                    //从数据中获取 曝光日志标记
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //如果是曝光日志输出到曝光侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            //获取每一条曝光数据
                            JSONObject displaysJsonObject = displays.getJSONObject(i);
                            String pageID = displaysJsonObject.getJSONObject("page").getString("page_id");

                            displaysJsonObject.put("page_id", pageID);
                            context.output(displayTag, displaysJsonObject.toString());

                        }


                    } else {
                        //如果不是曝光日志，说明是页面日志，输出到主流
                        collector.collect(jsonObj.toString());
                    }


                }


            }
        });


        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("page>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>>>");

        //TODO 输出到kafka不同的主题
        pageDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));
        startDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
        displayDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));



        env.execute();
    }

}
