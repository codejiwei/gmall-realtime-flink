package com.codejiwei.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {
    //需要提供一个获取key的方法，但是这个方法如何实现不知道
    String getKey(T obj);

    //提供一个join方法，用来关联维度数据和事实数据
    void join(T obj, JSONObject dimInfoJsonObj) throws ParseException;
}
