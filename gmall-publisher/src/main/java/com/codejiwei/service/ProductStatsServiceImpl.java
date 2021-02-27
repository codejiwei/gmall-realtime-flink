package com.codejiwei.service;

import com.codejiwei.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Author: codejiwei
 * Date: 2021/2/27
 * Desc: 商品统计service接口实现类
 *
 * TODO IOC 容器      创建对象一开始是程序员创建对象，现在对对象的控制交给IOC容器控制
 *  控制反转
 *  IOC帮我们创建对象，通过添加一个@Service注解
 **/
@Service    //标识是Spring的Service层组件，将对象的创建交给Spring的IOC容器
public class ProductStatsServiceImpl implements ProductStatsService{

    //TODO 在容器中，寻找ProductStatsMapper类型的对象，赋值给当前属性
    // TODO 通过IOC创建对象，使用Autowired进行注入
    @Autowired
    ProductStatsMapper productStatsMapper;  //调整IDEA的提醒等级为Warn

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }
}
