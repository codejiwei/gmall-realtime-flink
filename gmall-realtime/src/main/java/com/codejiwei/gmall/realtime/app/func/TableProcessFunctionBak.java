package com.codejiwei.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MySQLUtilBak;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @ClassName TableProcessBak
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/2 14:20
 * @Version 1.0
 **/
/*
    要读取MySQL配置表，需要建立ORM

    open()
        1 周期性的读取MySQL配置表数据，2 写入到Map内存， 3 周期型检测phoenix建表


    processElement()
*/
public class TableProcessFunctionBak extends ProcessFunction<JSONObject, JSONObject> {
    //1 声明一个Map集合用来存放MySQL配置数据
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();

    //2 声明一个Set集合用来存放Phoenix(HBase)中的表
    private Set<String> existTable = new HashSet<>();

    //3 声明一个Phoenix连接
    private Connection conn = null;

    //4 要数据分流 定义一个侧输出流标签
    OutputTag<JSONObject> outputTag;

    public TableProcessFunctionBak(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //TODO 初始化操作，第一条数据来的时候，周期执行
    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //用来1 周期性的读取MySQL配置表数据，2 写入到Map内存， 3 周期型检测phoenix建表
        refreshMeta();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 5000, 5000);

    }

    private void refreshMeta() {
        //1 读取MySQL配置表中的数据
        List<TableProcess> queryList = MySQLUtilBak.queryList("select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : queryList) {
            //拼接key source_table + operator_type
            String sourceTable = tableProcess.getSourceTable();
            String operateType = tableProcess.getOperateType();
            String key = sourceTable + ":" + operateType;

            //2 将查询的数据放到内存的Map中
            tableProcessMap.put(key, tableProcess);

            //3 判断phoenix（hbase）中是否存在sink的表，如果不存在建表
            if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE) && "insert".equals(tableProcess.getOperateType())) {

                //如果Set中有了那么就为false
                boolean notExist = existTable.add(tableProcess.getSinkTable());
                if (notExist) {
                    //如果是true，那么phoenix中就没有这张表
                    checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }

        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息！");
        }


    }

    //phoenix建表，类似于JDBC操作
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //1 处理主键为空和扩展字段为空
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //2 拼接建表语句
        StringBuilder createTableSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." + sinkTable + " (");
        //sink的表的字段是用,拼接的string，所以切割字符串
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            //判断主键
            if (sinkPk.equals(field)) {
                createTableSql.append(field).append(" varchar ");
                createTableSql.append(" primary key ");
            } else {
                //非主键字段
                createTableSql.append("info.").append(field).append(" varchar");
            }
            //处理查询总的逗号
            if (i < fields.length - 1) {
                createTableSql.append(", ");
            }
        }
        //最后添加 )
        createTableSql.append(")");
        createTableSql.append(sinkExtend);

        //3 创建phoenix操作对象
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createTableSql.toString());

            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("建表失败！！");
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    //TODO 每条数据来的时候执行
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        //1 获取每条来的数据的获取数据中的source_table和operator_type拼接成key和内存中的Map数据对比
        String tableName = jsonObj.getString("table");
        String OperatorType = jsonObj.getString("type");

        //获取数据中的data字段
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");


        //2 数据修复，因为Maxwell的bootstrap模式的insert会编程bootstrap-insert
        if ("bootstrap-insert".equals(OperatorType)) {
            OperatorType = "insert";
            jsonObj.put("type", OperatorType);
        }

        //3 与配置表保存到内存Map数据 做匹配
        if (tableProcessMap != null && tableProcessMap.size() > 0) {

            //拼接key
            String key = tableName + ":" + OperatorType;
            TableProcess tableProcess = tableProcessMap.get(key);
            if (tableProcess != null) {
                //该条数据从配置表中找到匹配的信息了
                //3.1 添加一个字段sink_table
                jsonObj.put("sink_table", tableProcess.getSinkTable());

                //3.2 如果指定了sinkColumn，需要对保留的字段进行过滤
                String sinkColumns = tableProcess.getSinkColumns();
                if (sinkColumns != null && sinkColumns.length() > 0){
                    filterColumn(dataJsonObj, sinkColumns);
                }

            } else {
                //如果该条信息没有从配置表中找到匹配的信息
                System.out.println("No this key " + key + "in MySQL");
            }

            //4 将数据进行分流: 维度数据分到侧输出流
            if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //如果sink到hbase，放到侧输出流输出
                ctx.output(outputTag, jsonObj);
            }else if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果sink到kafka，放到主流输出
                out.collect(jsonObj);
            }
        }


    }

    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        String[] columns = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columns);

        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        while (iterator.hasNext()){
            Map.Entry<String, Object> entry = iterator.next();
            if (!columnsList.contains(entry.getKey())){
                iterator.remove();
            }
        }


    }
}
