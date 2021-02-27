package com.codejiwei.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.app.func.DimAsyncFunction;
import com.codejiwei.gmall.realtime.bean.OrderWide;
import com.codejiwei.gmall.realtime.bean.PaymentWide;
import com.codejiwei.gmall.realtime.bean.ProductStats;
import com.codejiwei.gmall.realtime.common.GmallConstant;
import com.codejiwei.gmall.realtime.utils.ClickHouseUtil;
import com.codejiwei.gmall.realtime.utils.DateTimeUtil;
import com.codejiwei.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author: codejiwei
 * Date: 2021/2/23
 * Desc: 商品主题统计应用
 **/
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境准备
        //1.1 创建流处理执行环境
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
        //2.1 声明相关的主题名以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //2.2 从页面日志中获取点击和曝光数据
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));

//        pageViewDS.print("pageViewDS>>>>>>>>>>>>>>>>>>");


        //2.3 从dwd_favor_info中获取收藏数据
        DataStreamSource<String> favorInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));

        //2.4 从dwd_cart_info中获取加购数据
        DataStreamSource<String> cartInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));

        //2.5 从dwm_order_wide中获取订单信息数据
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //2.6 从dwm_payment_wide中获取支付信息数据
        DataStreamSource<String> paymentWideDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));

        //2.7 从dwd_order_refund_info中获取退款数据
        DataStreamSource<String> refundInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));

        //2.8 从dwd_comment_info中获取评论数据
        DataStreamSource<String> commentInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));


        //TODO 3 将各个流的数据格式转换成统一的ProductStats对象格式
        //3.1 对点击和曝光的数据进行转换  jsonStr -> ProductStats
        SingleOutputStreamOperator<ProductStats> productClickAndDisplayDS = pageViewDS.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                //将json格式字符串转换为json对象
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                String pageId = pageJsonObj.getString("page_id");
                if (pageId == null) {
                    System.out.println(">>>" + jsonObj);
                }
                //获取操作时间
                Long ts = jsonObj.getLong("ts");

                //封装点击信息，如何判断是点击操作呢？page_id为good_detail的表示对该商品的点击。
                if ("good_detail".equals(pageId)) {
                    //是对该商品的点击了，那么封装一次点击操作
                    //获取被点击商品的id
                    Long skuId = pageJsonObj.getLong("item");
                    //封装一次点击操作
                    ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                    //向下游输出
                    out.collect(productStats);
                }

                //封装曝光信息
                JSONArray displays = jsonObj.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    //说明从日志数据中获取到了曝光数据
                    for (int i = 0; i < displays.size(); i++) {
                        //获取曝光数据
                        JSONObject displaysJsonObj = displays.getJSONObject(i);
                        //判断是否曝光的是一个商品
                        if ("sku_id".equals(displaysJsonObj.getString("item_type"))) {
                            //获取商品id
                            Long skuId = displaysJsonObj.getLong("item");
                            //封装曝光商品对象
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                            //向下游输出
                            out.collect(productStats);
                        }
                    }
                }
            }
        });
//        productClickAndDisplayDS.print("点击和曝光：》》》》》》》》》》》");

        //3.2 对收藏数据进行结构的转换
        SingleOutputStreamOperator<ProductStats> productFavorInfoDS = favorInfoDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.getTs(jsonObj.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(ts)
                        .build();
                return productStats;
            }
        });
//        productFavorInfoDS.print("收藏》》》》》》》》》》");

        //3.3 对加购数据进行结构的转换
        SingleOutputStreamOperator<ProductStats> productCartDS = cartInfoDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                Long skuId = jsonObj.getLong("sku_id");
                Long ts = DateTimeUtil.getTs(jsonObj.getString("create_time"));
                ProductStats productStats = ProductStats.builder().sku_id(skuId).cart_ct(1L).ts(ts).build();
                return productStats;
            }
        });

//        productCartDS.print("加购物车》》》》》》》》》》》》》》");

        //3.4 对下单数据 进行结构的转换
        SingleOutputStreamOperator<ProductStats> productOrderWideDS = orderWideDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                //将json字符串转换为对应的订单宽表对象
                OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                String createTime = orderWide.getCreate_time();
                //将时间字符串转换为毫秒数
                Long ts = DateTimeUtil.getTs(createTime);
                ProductStats productStats = ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(ts)
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .build();
                return productStats;
            }
        });
//        productOrderWideDS.print("下单》》》》》》》》》》》》》");

        //3.5 对支付数据 进行结构的转换
        SingleOutputStreamOperator<ProductStats> productPaymentDS = paymentWideDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                Long ts = DateTimeUtil.getTs(paymentWide.getPayment_create_time());
                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts)
                        .build();
            }
        });
//        paymentWideDS.print("支付》》》》》》》》》》");
        //3.6 对退款数据 进行结构的转换
        SingleOutputStreamOperator<ProductStats> productRefundDS = refundInfoDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                JSONObject refundJsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.getTs(refundJsonObj.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                        .sku_id(refundJsonObj.getLong("sku_id"))
                        .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                        .ts(ts)
                        .build();
                return productStats;
            }
        });
        //3.7 对评价数据 进行结构的转换
        SingleOutputStreamOperator<ProductStats> productCommonDS = commentInfoDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                JSONObject commonJsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.getTs(commonJsonObj.getString("create_time"));
                long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                ProductStats productStats = ProductStats.builder()
                        .sku_id(commonJsonObj.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(ts)
                        .build();
                return productStats;
            }
        });

        //TODO 4 将7条流进行合并
        DataStream<ProductStats> unionDS = productClickAndDisplayDS.union(
                productFavorInfoDS,
                productCartDS,
                productOrderWideDS,
                productPaymentDS,
                productRefundDS,
                productCommonDS
        );

        //TODO 5 设置watermark和事件时间字段
        SingleOutputStreamOperator<ProductStats> productWithTsDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ProductStats>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                return productStats.getTs();
                            }
                        })
        );

        //TODO 6 根据sku_id分组
        KeyedStream<ProductStats, Long> productKeyedDS = productWithTsDS.keyBy(new KeySelector<ProductStats, Long>() {
            @Override
            public Long getKey(ProductStats productStats) throws Exception {
                return productStats.getSku_id();
            }
        });

        //TODO 7 开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = productKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 8 对窗口中的数据进行聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(new ReduceFunction<ProductStats>() {
                      @Override
                      public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                          stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                          stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                          stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                          stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                          stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                          stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                          stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                          stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                          stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                          stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                          stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                          stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                          stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                          stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                          stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                          stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                          return stats1;
                          }
                      },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        //获取窗口的起始时间
                        for (ProductStats productStats : elements) {
                            productStats.setStt(sdf.format(context.window().getStart()));
                            productStats.setEdt(sdf.format(context.window().getEnd()));
                            productStats.setTs(new Date().getTime());
                            out.collect(productStats);
                        }
                    }
                });

//        reduceDS.print(">>>>>>>>>>>>>>>>>>");

        //TODO 9 维度关联
        //9.1 关联商品维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        //根据sku_id进行关联
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws ParseException {
                        //关联sku_name，sku_price，spu_id，spu_id，品牌tm_id，品类category3_id
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //9.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        //根据spu_id进行关联
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws ParseException {
                        //关联spu_name
                        productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //9.3 关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(
                productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        //根据品牌id进行关联
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws ParseException {
                        //关联tm_name
                        productStats.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //9.4 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
                productStatsWithTmDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        //根据品类id进行关联
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws ParseException {
                        //关联category3_name
                        productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));

                    }
                },
                60,
                TimeUnit.SECONDS
        );

        productStatsWithCategoryDS.print(">>>>>>>>>>>>>>>>");


        //TODO 增加，方便统计商品行为关键词
        productStatsWithCategoryDS.map(new MapFunction<ProductStats, String>() {
            @Override
            public String map(ProductStats productStats) throws Exception {
                return JSON.toJSONString(productStats);
            }
        })
                .addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));

        //TODO 10 sink到ClickHouse
        productStatsWithCategoryDS.addSink(
                ClickHouseUtil
                        .<ProductStats>getJdbcSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                        ));

        env.execute();
    }
}
