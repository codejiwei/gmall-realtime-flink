package com.codejiwei.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.app.func.DimAsyncFunction;
import com.codejiwei.gmall.realtime.bean.OrderDetail;
import com.codejiwei.gmall.realtime.bean.OrderInfo;
import com.codejiwei.gmall.realtime.bean.OrderWide;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName OrderWideApp
 * @Author codejiwei
 * @Date 2021/2/5 11:39
 * @Version 1.0
 * @Description TODO:
 **/
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流处理执行环境
        //1.1 准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
//        //1.3 设置Checkpoint
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //1.4 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //1.5 设置状态后端
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "orderWideApp"));
//
//        //1.6 重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        //1.7 设置登录用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //TODO 2 从Kafka获取DWD层的订单和订单明细数据
        //2.1 声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupID = "order_wide_group";
        //2.2 获取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupID);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);
        //2.3 获取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupID);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

//        orderInfoJsonStrDS.print("orderInfo...");
//        orderDetailJsonStrDS.print("orderDetail...");

        //TODO 3 对读取的数据进行结构转换JsonString --> OrderInfo | OrderDetail
        //3.1 转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String jsonStr) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });

        //3.2 转换订单明细数据结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String jsonStr) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

//        orderInfoDS.print("orderInfoDS....");
//        orderDetailDS.print("orderDetailDS....");

        //TODO 4 设置事件时间水位线
        //4.1 订单数据 指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        //4.2 订单明细数据 指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        //TODO 5 按照订单id进行分组 指定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //TODO 6 使用intervalJoin对订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        orderWideDS.print("orderWide...");

        //TODO 7 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        //获取用户生日
                        String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                        //定义日期转换工具类
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //将生日字符串转换为日期对象
                        Date birthdayDate = sdf.parse(birthday);
                        //获取生日日期的毫秒数
                        Long birthdayTs = birthdayDate.getTime();

                        //获取当前时间的毫秒数
                        Long curTs = System.currentTimeMillis();

                        //年龄毫秒数
                        Long ageTs = curTs - birthdayTs;
                        //转换为年龄
                        Long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();

                        //将维度中的年龄赋值给订单宽表中的属性
                        orderWide.setUser_age(age);

                        //将维度中的性别赋值给订单宽表中的属性
                        orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 8 关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        orderWide.setProvince_name(dimInfoJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 9 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 10 关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        orderWide.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 11 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject){
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 12 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 13 结果写入到Kafka_Sink（DWM层）
        //13.1 首先改变结构
        orderWideWithTmDS
                .map(orderWide -> JSON.toJSONString(orderWide))
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));


        env.execute();
    }
}
