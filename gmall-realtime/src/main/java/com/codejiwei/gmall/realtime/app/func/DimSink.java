package com.codejiwei.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @ClassName DimSink
 * @Description TODO 通过Phoenix向HBase中写入数据
 * @Author codejiwei
 * @Date 2021/2/3 11:40
 * @Version 1.0
 **/
public class DimSink extends RichSinkFunction<JSONObject> {

    //声明phoenix连接
    Connection conn = null;

    //TODO 初始化操作
    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //创建phoenix连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


    }

    //TODO 生成upsert语句，向HBase中生成数据
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取Phoenix要创建的表名
        String sink_table = jsonObj.getString("sink_table");
        //获取Phoenix中的数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //拼接upsert语句
        String upsertSql = genUpsertSql(sink_table.toUpperCase(), dataJsonObj);
        PreparedStatement ps = null;

        try {
            System.out.println(upsertSql);
            //创建Phoenix操作对象
            ps = conn.prepareStatement(upsertSql);

            ps.execute();

            //TODO 注意Phoenix需要手动的提交事务，MySQL是自动提交事务的
            conn.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("执行Sql失败！");
        } finally {
            if (ps != null){
                ps.close();
            }
        }

    }

    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        Set<String> keySet = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();

        //upsert into 命名空间.表名(列名...) values (值...)
        String upsertSql = "upsert into "+ GmallConfig.HBASE_SCHEMA+"." + tableName + "(" +
                StringUtils.join(keySet, ",")  + ")";

        String valueSql = " values ('" + StringUtils.join(values, "','" )+ "')";
        return upsertSql + valueSql;

    }
}
