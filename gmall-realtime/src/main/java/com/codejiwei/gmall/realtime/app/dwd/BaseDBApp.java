package com.codejiwei.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.app.func.DimSink;
import com.codejiwei.gmall.realtime.app.func.TableProcessFunction;
import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @ClassName BaseDBApp
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/1 19:15
 * @Version 1.0
 **/
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        //1.1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
//        //1.3 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);  //设置检查点连接延迟
//        env.setStateBackend(new FsStateBackend(GmallConfig.CHECKPOINT_FILE_HEAD + "baseDBApp"));
//
//        //1.4 重启策略  如果说没有开启检查点ck，那么重启策略就是noRestart，就是不重启
//        //             如果开启了ck，那么默认重启策略 会尝试自动帮你尝试重启，重启Integer.MaxValue
//        env.setRestartStrategy(RestartStrategies.noRestart());
//
//        //1.4 设置登录用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.接收kafka数据，过滤空值数据
        //2.1 定义kafka消费者的主题、消费者组
        String topic = "ods_base_db_m";
        String groupID = "ods_base_group";
        //2.2 从kafka主题中读取数据
        DataStreamSource<String> jsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupID));

//        jsonStrDS.print();

        //TODO 3. 对DS中的数据进行结构的转换 String -> JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        //TODO 4. 对数据进行ETL 如果table为空 或者data为空 或者长度<3 将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() >= 3;
                    return flag;
                });

//        filteredDS.print(">>>>>>>>>>");

        //TODO 5. 动态分流 事实表放到主流，写回到kafka的DWD层；维度表通过侧输出流，写入HBase
        //5.1 定义输出到HBase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 主流 写回到kafka的数据
        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDS.process(new TableProcessFunction(hbaseTag));

        //5.3 侧输出流 写入HBase的数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);


        kafkaDS.print("kafka>>>>>>>>>>>>>>>>>>>>");
        hbaseDS.print("hbase>>>>>>>>>>>>>>>>>>>>>>>");


        //TODO 6 .将侧输出流写入到Phoenix（HBase）
        hbaseDS.addSink(new DimSink());

        //TODO 7 将主流的数据kafkaDS 写入到Kafka

        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka序列化初始化");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        String sinkTopic = jsonObj.getString("sink_table");
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

                        return new ProducerRecord<>(sinkTopic, dataJsonObj.toString().getBytes());
                    }
                }
        );

        kafkaDS.addSink(kafkaSink);

        env.execute();
    }
}
