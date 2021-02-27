package com.codejiwei.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.bean.OrderWide;
import com.codejiwei.gmall.realtime.bean.PaymentInfo;
import com.codejiwei.gmall.realtime.bean.PaymentWide;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.DateTimeUtil;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @ClassName PaymentWideApp
 * @Author codejiwei
 * @Date 2021/2/21 17:24
 * @Version 1.0
 * @Description : 支付宽表处理主程序
 **/
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境准备
        //1.1 创建流式处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
//        //1.3 开启检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //1.4 设置检查点配置 - 超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //1.5 配置状态后端
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "paymentWideApp"));
//        //1.6 设置重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        //1.7 设置HDFS登录用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2 从kafka中读取数据
        //2.1 声明相关主题和消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentWide_app_group";
        //2.2 读取dwd_payment_info
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        //2.3 读取dwm_order_wide
        DataStreamSource<String> orderWideJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //TODO 3 结构转换 jsonStr -> PaymentInfo | OrderWide
        //3.1 支付信息payment_info结构转换
        SingleOutputStreamOperator<PaymentInfo> paymentInfoJsonObjDS = paymentInfoJsonStrDS.map(jsonStr -> JSONObject.parseObject(jsonStr, PaymentInfo.class));
        //3.2 订单宽表order_wide结构转换
        SingleOutputStreamOperator<OrderWide> orderWideJsonObjDS = orderWideJsonStrDS.map(jsonStr -> JSONObject.parseObject(jsonStr, OrderWide.class));

//        paymentInfoJsonObjDS.print("paymentInfo>>>>>>>>>>>>");
//        orderWideJsonObjDS.print("orderWide>>>>>>>>>>>>");

        //TODO 4 设置watermark以及提取事件时间字段
        //4.1 支付信息的watermark设置以及事件时间指定
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoJsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                //将字符串格式的时间转换为毫秒数
                                /*
                                 * 如果直接在这里使用simpleDateFormat去将字符串格式时间转换成Long，不会涉及到多线程的安全问题
                                 * 但是每次对字符串格式转换成long格式太麻烦可以封装成工具类。但是封装成工具类使用simpleDateFormat
                                 * 就会涉及到线程安全问题，jdk1.8使用dateTimeFormatter替换了。
                                 * */
//                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                                long ts = 0L;
//                                try {
//                                    Date date = sdf.parse(paymentInfo.getCallback_time());
//                                    ts = date.getTime();
//                                } catch (ParseException e) {
//                                    e.printStackTrace();
//                                }
//                                return ts;
                                Long ts = DateTimeUtil.getTs(paymentInfo.getCallback_time());
                                return ts;
                            }
                        })
        );
        //4.2 订单宽表的watermark设置 事件时间字段指定
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideJsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                return DateTimeUtil.getTs(orderWide.getCreate_time());
                            }
                        })
        );

        //TODO 5 对数据进行分组
        //5.1 支付信息分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);
        //5.2 订单宽表 分组
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //TODO 6 使用IntervalJoin关联两条流
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(1800))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        paymentWideDS.print(">>>>>>>>>>>>>>");

        //TODO 7 将数据写回到Kafka的dwm层
        paymentWideDS
                .map(paymentWideJsonObj -> JSON.toJSONString(paymentWideJsonObj))
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));


        env.execute();
    }
}
