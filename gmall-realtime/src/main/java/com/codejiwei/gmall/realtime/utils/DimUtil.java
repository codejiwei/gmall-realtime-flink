package com.codejiwei.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @ClassName DimUtil
 * @Author codejiwei
 * @Date 2021/2/20 1:42
 * @Version 1.0
 * @Description :用于维度查询的工具类，底层调用的是PhoenixUtil
 * select * from dim_base_trademark where id=10 and name=zs;
 * <p>
 * PhoenixUtil是 通过传入一个sql去Phoenix中查询数据的！
 * <p>
 * DimUtil是 通过传入表名+where查询条件 拼接成SQL，然后调用Phoenix去查询数据！
 **/
public class DimUtil {
    //TODO 1 从Phoenix中查询数据，没有使用缓存
    //因为where条件不止一个所以使用可变长参数~
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        String whereSql = " where ";

        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String filedName = tuple2.f0;
            String filedValue = tuple2.f1;
            //如果有多个where条件
            if (i > 0) {
                whereSql += " and ";
            }
            whereSql += filedName + "='" + filedValue + "'";
        }

        String sql = "select * from " + tableName + whereSql;

        //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条。
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);

        JSONObject dimJsonObj = null;
        if (dimList != null && dimList.size() > 0) {
            dimJsonObj = dimList.get(0);
        } else {
            System.out.println("维度数据没有找到：" + sql);
            System.out.println();
        }
        return dimJsonObj;
    }

    //TODO 2 在做维度关联的时候，大部分场景都是通过id进行关联，所以提供一个方法，只需要将id的值作为参数传进来即可
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfoNoCache(tableName, Tuple2.of("id", id));
    }

    //TODO 3 优化方案：从Phoenix中查询，加入旁路缓存：先从缓存中查询，查询不到的再去Phoenix中查询，并将查询结果放到缓存中
    /*
        缓存在Redis中的数据类型是什么样的呢？
        String List Set Hash ZSet
        这里我们选择的是String
        类型：String
        key: dim:表名:值   例如：dim:DIM_BASE_TRADEMARK:10_xxx
        value: 通过PhoenixUtil到维度表中查询数据，取出第一条将其转换为json字符串
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //3.1 拼接Redis中的key
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        //3.2 拼接Phoenix中的查询语句
        String whereSql = " where ";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String fieldName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if (i > 0) {
                whereSql += " and ";
                redisKey += "_";
            }
            whereSql += fieldName + "='" + fieldValue + "'";

            redisKey += fieldValue;
        }

        //3.3 从Redis中查询数据
        Jedis jedis = null;
        //维度数据的json字符串形式
        String dimJsonStr = null;
        //维度数据的json对象形式
        JSONObject dimJsonObj = null;
        try {
            jedis = RedisUtil.getJedis();
            dimJsonStr = jedis.get(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Redis中查询维度失败！");
        }

        //判断是否从Redis中查询到数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            dimJsonObj = JSON.parseObject(dimJsonStr);
        }else {
            //没有从Redis中查询到数据，那么就去Phoenix中查询数据
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的SQL：" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度数据一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (dimList != null && dimList.size() > 0) {
                dimJsonObj = dimList.get(0);
                //如果从Phoenix中查询到数据，将查询结果缓存到Redis中去
                if (jedis != null){
                    jedis.setex(redisKey, 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("维度数据没有找到：" + sql);
            }
        }

        //关闭Jedis
        if (jedis != null){
            jedis.close();
        }
        return dimJsonObj;
    }


    //根据key让Redis中的缓存失效
    public static void deleteCached(String tableName, String id ) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            //通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {
        //TODO 0 如果直接使用PhoenixUtil工具类通过传入sql的方式进行查询：
//        System.out.println(PhoenixUtil.queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));

//        //TODO 1 如果使用DimUtil工具类方式进行查询
//        JSONObject dimInfoNoCache = DimUtil.getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "14"));
//        System.out.println(dimInfoNoCache);

//        //TODO 2 如果使用DimUtil工具类 只传入参数id的值 进行查询
//        JSONObject dimInfo = DimUtil.getDimInfo("DIM_BASE_TRADEMARK", "14");
//        System.out.println(dimInfo);

        //TODO 3 使用Redis做旁路缓存 进行查询
        JSONObject dimInfo = DimUtil.getDimInfo("DIM_BASE_TRADEMARK", Tuple2.of("id", "14"));
        System.out.println(dimInfo);

    }

}
