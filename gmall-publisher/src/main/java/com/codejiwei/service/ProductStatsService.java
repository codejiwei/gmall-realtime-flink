package com.codejiwei.service;

import java.math.BigDecimal;

/**
 * Author: codejiwei
 * Date: 2021/2/27
 * Desc: 商品统计的service接口
 *  TODO Service做业务处理
 **/
public interface ProductStatsService {
    //获取某一天交易总额

    BigDecimal getGMV(int date);
}
