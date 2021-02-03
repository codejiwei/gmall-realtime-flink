package com.codejiwei.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.codejiwei.gmall.realtime.bean.TableProcess;
import com.codejiwei.gmall.realtime.common.GmallConfig;
import com.codejiwei.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.security.Key;
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
public class TableProcessFunctionBak extends ProcessFunction<JSONObject, JSONObject> {
    //因为要将维度数据写到侧输出流，所以定义一个侧输出流标签
    private OutputTag<JSONObject> outputTag;

    //定义一个Map，用来在内容中 存放查询结果
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();

    //定义一个Set，用来在内存中 存放hbase中的表名
    private Set<String> existsTables = new HashSet<>();

    //初始化Phoenix连接
    private Connection connection = null;


    //TODO 1 当第一次来的时候执行一次，初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        //TODO 1.3 注册驱动，创建phoenix连接，只需要初始化一次！所以放在open方法中
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


        //1.1 初始化配置表信息
        refreshMeta();

        //TODO 1.2 周期性调度
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 5000, 5000);

    }

    //TODO 2 周期型查询配置表
    private void refreshMeta() {
        //TODO 2.1 查询配置表信息
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            //获取源 表名
            String sourceTable = tableProcess.getSourceTable();
            //获取源 操作类型
            String operateType = tableProcess.getOperateType();
            //获取 sink的输出类型 hbase / kafka
            String sinkType = tableProcess.getSinkType();
            //获取 sink的输出表名 / 主题名
            String sinkTable = tableProcess.getSinkTable();
            //获取 sink的输出字段
            String sinkColumns = tableProcess.getSinkColumns();
            //获取 sink的主键
            String sinkPk = tableProcess.getSinkPk();
            //获取 sink的拓展语句
            String sinkExtend = tableProcess.getSinkExtend();

            //TODO 2.2 拼接key，缓存到内存的map中
            String key = sourceTable + ":" + operateType;
            tableProcessMap.put(key, tableProcess);

            //TODO 2.3 如果是向HaBase中保存的表，那么判断在内存中是否有改表名
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
                //如果是sink到hbase，并且操作类型是insert
                boolean notExist = existsTables.add(sinkTable);
                if (notExist) {
                    //如果内存中不存在这个表，那么需要去phoenix创建表
                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }

            }

        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息！");
        }
    }

    //TODO 3 拼接建表语句，使用phoenix建表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //TODO 3.1 主键为空，或扩展字段为空
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }


        //TODO 3.2 拼接建表sql
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

        //TODO 3.3 创建phoenix操作对象
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(createTableSql.toString());
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
    public void processElement(JSONObject jsonObj, Context context, Collector<JSONObject> collector) throws Exception {
        //从数据中获取sink到哪张表
        String table = jsonObj.getString("table");
        //从数据中获取操作的类型
        String type = jsonObj.getString("type");
        //从数据中获取data
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //TODO 还需要做一下修复！因为maxwell的bootstrap获取数据库中原有的数据的时候，会改变type
        if ("bootstrap-insert".equals(type)){
            type = "insert";
            jsonObj.put("type", type);
        }

        if (tableProcessMap != null && tableProcessMap.size() > 0){
            //拼接内存中保存的key，判断当前的key是否在内存中存在
            String key = table + ":" + type;
            TableProcess tableProcess = tableProcessMap.get(key);

            //如果能够在内存的map中获得，那么说明这个值在内存中有
            if (tableProcess != null){
                //那么就给传进来的数据添加一个字段：sink到哪张表中？
                jsonObj.put("sink_table", tableProcess.getSinkTable());

                //TODO 如果指定了sink的column，那么也需要对多余的列 清洗
                String sinkColumns = tableProcess.getSinkColumns();
                if (sinkColumns != null && sinkColumns.length() > 0){
                    filterColumn(jsonObj.getJSONObject("data"), sinkColumns);
                }

            }else {
                //如果内存中没有
                System.out.println("No This Key " + key + "in MySQL!");
            }

            //TODO 根据sinkType，将数据输出到不同的流中
            if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //如果是维度数据，输出到hbase
                context.output(outputTag, jsonObj);
            }else if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果是实时数据，输出到主流 kafka
                collector.collect(jsonObj);
            }

        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        //清洗数据

        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())){
                iterator.remove();
            }
        }
    }
}
