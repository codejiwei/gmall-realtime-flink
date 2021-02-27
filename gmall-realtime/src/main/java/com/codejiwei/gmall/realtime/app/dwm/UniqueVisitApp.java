package com.codejiwei.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName UniqueVisitApp
 * @Description TODO 独立访客统计
 * @Author codejiwei
 * @Date 2021/2/4 11:29
 * @Version 1.0
 * TODO 流程：模拟日志jar -> nginx -> 3个日志服务器 -> kafka(ods_base_log) -> BaseLogApp(动态分流)
 *             -> kafka(dwd_page|start|display_log) -> UniqueVisitApp -> dwm
 *
 **/
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流处理执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.1 设置并行度
        env.setParallelism(4);
//        //1.2 开启检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        //1.3 设置状态后端
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "uniqueVisitApp"));
//
//        //1.4 设置登录用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        //1.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());


        //TODO 2 从kafka的DWD层读取数据


        String SourceTopic = "dwd_page_log";
        String groupID = "dwm_group";
        DataStreamSource<String> kafkaSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(SourceTopic, groupID));

//        kafkaSourceDS.print(">>>>>>>>>>>>>>>>");

        //TODO 3 转换结构：string -> jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.map(jsonStr -> JSONObject.parseObject(jsonStr));

        //TODO 4 根据mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5 过滤独立访客
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {

            //1 定义ValueState状态，对一天中的重复访问去重
            ValueState<String> lastVisitDateState = null;

            //2 因为要根据ts字段转换成yyyyMMdd判断是否是一天的重复访问，所以定义日期解析
            SimpleDateFormat sdf = null;

            //3 初始化 状态和日期解析工具类
            @Override
            public void open(Configuration parameters) throws Exception {

                //3.1 初始化日期工具类
                sdf = new SimpleDateFormat("yyyyMMdd");
                //3.2 初始化状态
                ValueStateDescriptor<String> firstVisitStateDes = new ValueStateDescriptor<>("firstVisit", String.class);
                //TODO 设置状态失效时间！
                //因为我们统计的是日活DAU，所以状态数据只在当天有效，过了一天就失效
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                firstVisitStateDes.enableTimeToLive(stateTtlConfig);
                lastVisitDateState = getRuntimeContext().getState(firstVisitStateDes);

            }

            //4 核心过滤
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                //4.1 过滤独立掉last_page_id != null 的数据
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");

                if (lastPageId != null && lastPageId.length() == 0) {
                    return false;
                }

                //4.2 获取数据中的ts字段
                Long logTS = jsonObj.getLong("ts");
                //4.3 格式化日期
                String logDate = sdf.format(new Date(logTS));
                //4.4 获取状态中的日期
                String lastVisitDate = lastVisitDateState.value();

                //如果last_page_id为null，说明是首次访问，那么就需要去重 --> 状态
                //如何去重？ 根据ts字段，如果状态不为null，说明有一天的重复访问
                //          那么要判断数据的ts字段，判断yyyyMMdd，如果是一天那么就去掉
                //4.5 用当前日期和状态日期进行对比
                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                    System.out.println("已访问：lastVisitDate-" + lastVisitDate + ", || logDate-" + logDate);
                    //如果一天中有重复访问
                    return false;
                } else {
                    //如果状态中没有日期并且，日期没有重复，放到状态中
                    lastVisitDateState.update(logDate);
                    return true;
                }
            }
        });

//        filterDS.print(">>>>>>>>>>>>>>");

        //TODO 6 转换结构 jsonObject -> jsonStr
        SingleOutputStreamOperator<String> jsonStrDS = filterDS.map(jsonObj -> jsonObj.toJSONString());

        //TODO 7 将过滤处理后的UV数据写回到Kafka的DWM层

        String sinkTopic = "dwm_unique_visit";
        jsonStrDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
