package com.codejiwei.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * Author: codejiwei
 * Date: 2021/2/27
 * Desc: 编写SQL查询商品统计表 接口：ssm帮助创建实现类
 *  TODO mapper就和数据库打交道
 **/
public interface ProductStatsMapper {
    //获取某一天商品的交易额
    //TODO SSM底层 通过@Select实现抽象方法    sql语句中有抽象方法传入的参数 使用#{参数名} 的方式使用参数
    @Select("select sum(order_amount) from product_stats_2021 where toYYYYMMDD(stt)=#{date};")
    BigDecimal getGMV(int date);
}
