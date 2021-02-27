package com.codejiwei.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName BaseLogAPP
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/1/31 21:07
 * @Version 1.0
 **/
public class BaseLogAPP {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";

    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1 创建flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度，最好要与kafka的分区数一致
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //每5000ms做一次checkpoint，模式为 EXACTLY_ONCE（默认）
//        env.enableCheckpointing(5000);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/baselogApp"));
//
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.从kafka中读取数据(kafka消费者)
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>()
        String topic = "ods_base_log";
        String groupID = "base_log_app_group";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupID));

        //TODO 3.对读取到的数据格式进行转换String -> Json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });

//        jsonObjDS.print("json>>>>>>>>>>>>>>>>>>>>>>>>");

        //TODO 4.识别新老访客
        //前端也会对新老访客状态做一个记录，但是有可能不准（比如，服务器重启）
        //保存mid某天方法情况，（将首次访问日期作为状态保存起来），等后面该设备在有日志过来的时候，从状态中获取日期
        //和当前日志的日期做对比，如果状态不为空，并且当前日期和状态日期不相等，说明是老访客，如果is_new=1，那么对状态进行修复

        //4.1 根据mid对日志进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //4.2 新老访客修复，状态分为算子状态和键控状态，我们这里要记录每一个设备的访问，使用键控状态比较合适
        SingleOutputStreamOperator<JSONObject> jsonDSwithFlag = midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            //定义mid的首次访问日期的状态
            private ValueState<String> firstVisitDateState;
            //定义日期格式化对象
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                //对状态以及日期的格式进行初始化
                firstVisitDateState = getRuntimeContext().getState(
                        new ValueStateDescriptor<String>("newMidDateState", String.class));
                sdf = new SimpleDateFormat("yyyyMMdd");

            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //获取当前日志标记状态
                String isNew = jsonObj.getJSONObject("common").getString("is_new");

                //获取当前日志访问时间戳
                Long ts = jsonObj.getLong("ts");

                //外面这层判断，是仅从当前的is_new=1的数据中处理，因为只有is_new=1的才需要修复
                if ("1".equals(isNew)) {
                    //获取当前mid对象的状态
                    String stateDate = firstVisitDateState.value();
                    //对当前条日志的日期格式进行格式化
                    String curDate = sdf.format(new Date(ts));

                    //如果当前状态不为空，并且状态的日期和当前数据的日期不相等，说明是个老访客
                    if (stateDate != null && stateDate.length() != 0) {
                        //判断是否为同一天数据
                        if (!stateDate.equals(curDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }

                    } else {
                        //如果当前状态为空，那么将当前的访问日期，作为状态值
                        firstVisitDateState.update(curDate);
                    }

                }

                return jsonObj;
            }
        });
//
//        jsonDSwithFlag.print(">>>>>>>>>>>>>>>>>>>>>>");



        //TODO 5 分流 根据日志数据内容，将日志数据分为3类，页面日志，启动日志，曝光日志
        //页面日志数据出到主流，启动日志输出到启动侧输出流曝光日志输出到曝光测输出流
        //侧输出流： (1)接收迟到数据    (2)分流

        //定义启动侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };

        //定义曝光侧输出流
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonDSwithFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context context, Collector<String> collector) throws Exception {
                //从数据中获取启动日志标记
                JSONObject startJsonObj = jsonObj.getJSONObject("start");

                //将jsonObject格式转换为字符串，方便向侧输出流输出以及向kafka中写入
                String dataStr = jsonObj.toString();

                //判断是否为启动日志
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    //如果是启动日志，输出到启动侧输出流
                    context.output(startTag, dataStr);
                } else {
                    //TODO 如果不是启动日志，说明是页面日志（页面包含曝光数据！！！），输出到主流
                    collector.collect(dataStr);

                    //如果不是启动日志，获取曝光日志标记
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    //判断是否为曝光日志
                    if (displays != null && displays.size() > 0) {
                        //如果是曝光日志，遍历输出每一条曝光记录，到曝光侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            //获取每一条曝光事件
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            //获取页面id
                            String pageId = jsonObj.getJSONObject("page").getString("page_id");
                            //给每一条曝光事件添加pageId
                            displayJsonObj.put("page_id", pageId);
                            context.output(displayTag, displayJsonObj.toString());
                        }
                    }

                }

            }
        });

        //获取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //打印输出
        pageDS.print("page>>>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>>>>>");

        //TODO 6 将数据输出到kafka不同的主题中
        pageDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));
        startDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
        displayDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));

        env.execute();
    }
}
