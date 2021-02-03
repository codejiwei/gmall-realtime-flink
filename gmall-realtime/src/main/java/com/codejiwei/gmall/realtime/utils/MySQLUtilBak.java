package com.codejiwei.gmall.realtime.utils;

import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.checkerframework.common.reflection.qual.NewInstance;

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
    public static <T> List<T> queryList(String sql, Class<T> clazz, boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        //JDBC操作六步走
        try {
            //1 获取驱动
            Class.forName("com.mysql.jdbc.Driver");

            //2 建立JDBC连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_realtime_flink_property?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");

            //3 创建数据库操作对象
            ps = conn.prepareStatement(sql);

            //4 执行SQL
            rs = ps.executeQuery();

            //5 处理结果集
            //5.1 获取结果集的元数据
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            //5.2 声明一个封装结果的list
            List<T> resultList = new ArrayList<>();

            while (rs.next()){
                //通过反射获取对象
                T obj = clazz.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = md.getColumnName(i);
                    //判断是否开启转换驼峰命名
                    if (underScoreToCamel){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //给对象更新属性
                    BeanUtils.setProperty(obj, columnName, rs.getObject(i));
                }
                resultList.add(obj);

            }
            return resultList;


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询MySQL失败！");
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
