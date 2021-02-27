package com.codejiwei.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName PhoenixUtil
 * @Author codejiwei
 * @Date 2021/2/20 1:24
 * @Version 1.0
 * @Description :从Phoenix中查询数据
 **/
public class PhoenixUtil {
    private static Connection conn = null;

    public static void init() {
        try {
            //1 注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //2 获取Phoenix的连接
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //指定操作的表空间
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //从Phoenix中查询数据
    //select * from 表 where XXX=xxx
    public static <T> List<T> queryList(String sql, Class<T> clazz) {

        if (conn == null){
            init();
        }
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //3 获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //4 指定SQL语句
            rs = ps.executeQuery();

            //通过结果集对象获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //5 处理结果集
            while (rs.next()){
                //5.1 声明一个对象，用来封装查询的一条结果集
                T rowData = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++ ){
                    BeanUtils.setProperty(rowData, metaData.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从维度表中查询数据失败");
        } finally {
            //6 释放资源
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (ps != null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        return resultList;
    }

    public static void main(String[] args) {
        System.out.println(queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
    }
}
