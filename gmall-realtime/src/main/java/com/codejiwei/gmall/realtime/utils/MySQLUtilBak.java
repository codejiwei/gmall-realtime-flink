package com.codejiwei.gmall.realtime.utils;

import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MySQLUtilBak
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/1 21:30
 * @Version 1.0
 **/
public class MySQLUtilBak {
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;


        //TODO JDBC操作六步走

        try {
            //TODO 1 注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //TODO 2 建立连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_realtime_flink_property?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");

            //TODO 3 创建数据库操作对象
            ps = conn.prepareStatement(sql);

            //TODO 4 执行SQL
            rs = ps.executeQuery();

            //TODO 5 处理结果集
            //5.1 获取元数据信息，列名、列的行数等。。
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
//            metaData.getColumnName()

            //5.2 声明一个结果集合
            List<T> resultList = new ArrayList<>();

            while (rs.next()){
                T obj = clazz.newInstance();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    //如果开启了下划线转驼峰命名
                    if (underScoreToCamel){
                        //直接调用Google的guava的CaseFormat转换
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的common-bean中的工具类。给对象赋值
                    BeanUtils.setProperty(obj, columnName, rs.getObject(i));

                }
                resultList.add(obj);
            }
            return resultList;


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("sql 查询失败！");
        } finally {
            //TODO 6 释放资源
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
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
