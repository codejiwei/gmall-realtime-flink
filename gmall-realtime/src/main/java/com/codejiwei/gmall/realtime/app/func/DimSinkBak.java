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
 * @ClassName DimSinkBak
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/3 22:48
 * @Version 1.0
 **/
public class DimSinkBak extends RichSinkFunction<JSONObject> {
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //1 注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //2 建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //3 获取要创建的upsert语句中的信息
        String sink_table = jsonObj.getString("sink_table");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //4 拼接upsert语句
        String upsertSql = genUpsertSql(sink_table.toUpperCase(), dataJsonObj);


        //5 创建phoenix连接对象
        PreparedStatement ps = null;
        try {
            System.out.println("插入phoenix的SQL语句：");
            ps = conn.prepareStatement(upsertSql);
            ps.execute();

            //注意phoenix需要手动提交事务
            conn.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("执行SQL失败");
        } finally {
            if (ps != null) {
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
