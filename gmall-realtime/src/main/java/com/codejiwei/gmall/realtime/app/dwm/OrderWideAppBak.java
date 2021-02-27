package com.codejiwei.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.codejiwei.gmall.realtime.bean.OrderDetail;
import com.codejiwei.gmall.realtime.bean.OrderInfo;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @ClassName OrderWideAppBak
 * @Author codejiwei
 * @Date 2021/2/20 0:22
 * @Version 1.0
 * @Description TODO:
 **/
public class OrderWideAppBak {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 设置并行度
        env.setParallelism(4);
        //1.2 开启检查点ck
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        //1.3 配置检查点延迟时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.4 配置状态后端
        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "orderWideApp"));
        //1.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());
        //1.6 设置登录HDFS用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2 从kafka的dwd层中读取数据
        //2.1 设置kafka的主题和消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupID = "order_wide_group";

        //2.2 获取订单数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupID);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);

        //2.2 获取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupID);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);


        //TODO 3 转换结构JsonStr -> OrderInfo| OrderDetail
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

        orderInfoDS.print("orderInfo...");
        orderDetailDS.print("orderDetail...");

        env.execute();
    }
}
