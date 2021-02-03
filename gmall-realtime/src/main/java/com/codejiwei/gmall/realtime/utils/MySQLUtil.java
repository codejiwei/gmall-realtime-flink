package com.codejiwei.gmall.realtime.utils;

import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MySQLUtil
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/1 20:21
 * @Version 1.0
 **/
public class MySQLUtil {
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        //TODO JDBC操作六步走
        try {
            //TODO 1注册驱动
            Class.forName("com.mysql.jdbc.Driver");

            //TODO 2获取JDBC连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/gmall_realtime_flink_property?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456"
            );

            //TODO 3创建数据库操作对象
            ps = conn.prepareStatement(sql);

            //TODO 4执行SQL
            rs = ps.executeQuery();

            //TODO 5处理结果
            //5.1 获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();    //表中列数
//            metaData.getColumnName()      //获取每一列的列名
//            rs.getObject()    //获取结果的值

            //5.2 声明集合对象，用来封装返回结果
            List<T> resultList = new ArrayList<>();

            //5.3 遍历结果集，获取一条一条的查询结果
            while (rs.next()){
                //5.4 通过反射创建要查询结果 要转换为的目标类型对象
                T obj = clazz.newInstance();


                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);

                    //如果开启了下划线转驼峰的映射，那么转换为驼峰命名规则
                    if (underScoreToCamel) {
                        //直接调用Google的guava的CastFormat
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的common-bean中的工具类，给bean的属性赋值
                    BeanUtils.setProperty(obj, columnName, rs.getObject(i));

                }

                resultList.add(obj);

            }
            return resultList;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询MySQL失败！");
        } finally {
            //TODO 6释放资源
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
