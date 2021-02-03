package com.codejiwei.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.app.func.DimSink;
import com.codejiwei.gmall.realtime.app.func.TableProcessFunctionBak;
import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtilBak;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @ClassName BaseDBAppBak
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/3 20:40
 * @Version 1.0
 **/
public class BaseDBAppBak {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 设置并行度 和kafka的分区数相同
        env.setParallelism(4);

        //1.2 设置检查点
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.3 设置状态后端HDFS
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint"));

        //1.4 设置HDFS等于用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2 从kafka中获取数据
        String topic = "ods_base_db_m";
        String groupID = "ods_base_group";
        DataStreamSource<String> kafkaSourceDS = env.addSource(MyKafkaUtilBak.getKafkaSource(topic, groupID));

        //TODO 3 结构转换String -> JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.map(dataStr -> JSONObject.parseObject(dataStr));

//        jsonObjDS.print(">>>>>>>>>>>");
        //TODO 4 过滤空数据
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                boolean flag = jsonObj.getString("table") != null
                        && jsonObj.getJSONObject("data") != null
                        && jsonObj.getJSONObject("data").toString().length() > 3;
                return flag;
            }
        });
//        filteredDS.print(">>>>>>");

        //TODO 5 实现动态分流
        //  维度表放到 hbase ； 事实表放到 kafka
        //  维度表 通过侧输出流输出   ； 事实表 通过主流输出
        //通过维护一张mysql的配置表

        //5.1 定义侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 获取主流数据 kafka
        SingleOutputStreamOperator kafkaDS = filteredDS.process(new TableProcessFunctionBak(hbaseTag));

        //5.3 获取侧输出流数据 hbase
        DataStream hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("kafka>>>>>>>>>>");
        hbaseDS.print("hbase>>>>>>>>>>>");

        //TODO 6 sink到Phoenix(HBase)的表中
        hbaseDS.addSink(new DimSink());

        //TODO 7 sink到Kafka(多个不同的主题)
        kafkaDS.addSink(MyKafkaUtilBak.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("kafka序列化");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                String sink_topic = jsonObj.getString("sink_table");
                JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                return new ProducerRecord<>(sink_topic, dataJsonObj.toString().getBytes());
            }
        }));

        env.execute();
    }
}
