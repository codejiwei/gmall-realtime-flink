package com.codejiwei.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MySQLUtil;
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
 * @ClassName TableProcessFunction
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/1 21:24
 * @Version 1.0
 **/
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    //TODO 定义一个侧输出流
    private OutputTag<JSONObject> outputTag;

    //TODO 定义一个Map，用于在内存中保存配置表中的配置
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();

    //TODO 定义一个Set，用来在内存中保存 HBase中的表
    private Set<String> existsTables = new HashSet<>();


    private Connection conn = null;

    //TODO构造器
    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    public TableProcessFunction() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //TODO 使用phoenix创建表，创建连接放在open方法中
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //创建phoenix连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


        //TODO 周期型查询配置表
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

        //TODO 1 查询 mysql查询配置表的结果集合
        System.out.println("查询配置表信息");

        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            //获取源数据表名
            String sourceTable = tableProcess.getSourceTable();
            //获取源数据操作类型
            String operateType = tableProcess.getOperateType();
            //获取结果输出类型
            String sinkType = tableProcess.getSinkType();
            //获取结果输出表名
            String sinkTable = tableProcess.getSinkTable();
            //获取结果输出字段
            String sinkColumns = tableProcess.getSinkColumns();
            //获取结果输出主键
            String sinkPk = tableProcess.getSinkPk();
            //获取结果输出拓展语句
            String sinkExtend = tableProcess.getSinkExtend();

            //TODO 拼接key：表名：操作类型
            String key = sourceTable + ":" + operateType;
            //TODO 2 将数据存入结果Map
            tableProcessMap.put(key, tableProcess);

            //TODO 3 如果是向HBase中保存的表，那么判断表是否存在
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)){
                //如果配置表中 是输出到hbase，并且操作的类型是insert操作，就放到内存的set中
                //如果添加成功，那么就说明这个表 不存在在set内存中。
                boolean notExist = existsTables.add(sourceTable);

                if (notExist){
                    //如果不存在
                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }


            }


        }
        if (tableProcessMap == null || tableProcessMap.size() == 0){
            throw new RuntimeException("缺少处理信息");
        }


    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //TODO 如果内存set中不存在表，怎么处理表？
        //主键和拓展字段有可能为null，需要赋初始值
        if (sinkPk == null){
            //如果主键为null，设为主键为 id
            sinkPk = "id";
        }
        if (sinkExtend == null){
            //如果拓展字段为null，设为空字符串
            sinkExtend = "";
        }

        //如果set中不存在那么就，创建表
        StringBuilder createTableSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." + sinkTable + " (");
        //sink的字段是一个用,分割的字符串，所以需要切割字符串，获取一个一个字段
        String[] fields = sinkColumns.split(",");
        //将一个个字段拼接到建表语句中(不适用增加for循环 是因为有标点符号拼接)
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            //判断是否是主键
            if (sinkPk.equals(field)){
                //如果是主键
                createTableSql.append(field).append(" varchar");
                createTableSql.append(" primary key ");
            } else {
                //如果不是主键
                createTableSql.append("info.").append(field).append(" varchar");
            }
            if (i < fields.length - 1){
                createTableSql.append(",");
            }
        }
        createTableSql.append(")");
        //拼接扩展语句
        createTableSql.append(sinkExtend);

        System.out.println("创建Phoenix表的语句:" + createTableSql);


        //TODO 使用phoenix创建表
        //创建数据库操作对象
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createTableSql.toString());

            ps.execute();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("建表失败！！！");
        }finally {
            if (ps != null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    //TODO 每过来一个元素，方法执行一次，主要任务是根据内存中配置表Map对当前进来的元素进行分流
    @Override
    public void processElement(JSONObject jsonObj, Context context, Collector<JSONObject> collector) throws Exception {
        //从数据中获取表名
        String table = jsonObj.getString("table");
        //从数据中获取操作类型
        String type = jsonObj.getString("type");
        //从数据中获取数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //需要对数据做一些处理：因为如果使用Maxwell的bootstrap初始化数据库原来的数据，
        //那么insert的类型，会编程bootstrap-insert，所以我们修复为insert
        if ("bootstrap-insert".equals(type)){
            type = "insert";
            jsonObj.put("type", type);
        }

        //TODO 获取配置表的信息
        if (tableProcessMap != null && tableProcessMap.size() > 0){
            //根据表名和操作类型拼接为key
            String key = table + ":" + type;
            //从当前内存配置Map中获取当前key对象的配置信息
            TableProcess tableProcess = tableProcessMap.get(key);
            //如果获取到了该元素对应的配置信息
            if (tableProcess != null){
                //获取sinkTable，指明当前这条数据应该发往何处 如果是维度数据 那么对应的是phoenix中的表名；如果是实时数据 对应的是kafka的主题
                //TODO 获取配置信息中的sinkTable字段添加到数据中
                jsonObj.put("sink_table", tableProcess.getSinkTable());

                //TODO 如果指定了sinkColumn，需要对保留的字段进行过滤处理
                String sinkColumns = tableProcess.getSinkColumns();
                if (sinkColumns != null && sinkColumns.length() > 0){
                    filterColumn(jsonObj.getJSONObject("data"), sinkColumns);
                }
            }else {
                System.out.println("No this Key " + key + "in MySQL");
            }

            //TODO 根据sinkType，将数据输出到不同的流
            if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //如果是sink到hbase 那么就是维度数据 测流输出
                context.output(outputTag, jsonObj);
            }else if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果是sink到kafka 那么就是实时数据  主流输出
                collector.collect(jsonObj);
            }
        }


    }

    //TODO 对Data中数据进行过滤
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();

        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        while (iterator.hasNext()){
            Map.Entry<String, Object> entry = iterator.next();

            if (!columnList.contains(entry.getKey())){
                iterator.remove();
            }
        }
    }
}
