package com.codejiwei.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.utils.DimUtil;
import com.codejiwei.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.security.Key;
import java.text.ParseException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * @ClassName DimAsyncFunction
 * @Author codejiwei
 * @Date 2021/2/20 23:31
 * @Version 1.0
 * @Description :自定义维度异步查询的函数
 *  模板方法设计模式
 *      在父类中只定义方法的声明,让整个程序跑通
 *      具体的实现延迟到子类中实现
 **/
//两个泛型：IN：输入数据的类型；  OUT：输出数据的类型
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T>{

    //声明线程池对象的父接口（多态）
    private ExecutorService executorService;

    //维度的表名
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    /*
     * @Description //TODO 发送异步请求的方法
     * @Param [obj, resultFuture]: 流中的事实数据， 异步处理结束后，返回结果
     * @return void
     **/
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //从流中事实数据获取key
                    String key = getKey(obj);

                    //根据维度的主键到维度表中进行查询
                    JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                    System.out.println("维度数据json格式：" +  dimInfoJsonObj);

                    //如果从维度表中查询到结果，需要将事实数据和维度数据关联起来
                    if (dimInfoJsonObj != null) {
                        join(obj, dimInfoJsonObj);
                    }
                    System.out.println("维度关联后的对象：" + obj);


                    //将关联后的数据继续向下传递
                    resultFuture.complete(Arrays.asList(obj));
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new RuntimeException(tableName + "维度异步查询失败");
                }
            }
        });
    }
}
