package com.codejiwei.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.utils.DimUtil;
import com.codejiwei.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName DimAsyncFunctionBak
 * @Author codejiwei
 * @Date 2021/2/21 12:30
 * @Version 1.0
 * @Description :
 **/
public abstract class DimAsyncFunctionBak<T> extends RichAsyncFunction<T, T> {

    //声明线程池对象，面向接口编程，声明的是父接口对象（多态）
    private ExecutorService pool;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        pool = ThreadPoolUtil.getInstance();
    }

    //声明tableName成员变量
    private String tableName;
    //通过构造器的方式传入tableName
    public DimAsyncFunctionBak(String tableName) {
        this.tableName = tableName;
    }

    //定义一个获取key的抽象方法
    public abstract String getKey(T obj);

    //定义一个事实数据和维度数据关联的join抽象方法
    public abstract void join(T obj, JSONObject dimInfoJsonObj);

    /*
     * @Description //TODO 发送异步请求的方法
     * @Param [obj 流中的事实数据, resultFuture 异步处理结束后，返回结果]
     * @return void
     **/
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //调用线程池对象的submit方法，里面传入一个Runnable
        pool.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    //提供获得key的方式，因为要查询id=多少现在无法确定，根据流中的数据确定key
                    String key = getKey(obj);

                    //在run方法里面发送异步请求
                    //1 首先要去Phoenix中查询数据
                    JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                    System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                    //2 维度数据和流中的事实数据关联
                    if (dimInfoJsonObj != null && dimInfoJsonObj.size() > 0) {
                        join(obj, dimInfoJsonObj);
                    }
                    System.out.println("关联后的对象：" + obj);

                    //3 关联后需要将数据向下传递
                    resultFuture.complete(Arrays.asList(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(tableName + "维度异步查询失败！");
                }
            }
        });
    }
}
