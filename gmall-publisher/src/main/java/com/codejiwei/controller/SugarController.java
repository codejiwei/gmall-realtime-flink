package com.codejiwei.controller;

import com.codejiwei.service.ProductStatsService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Author: codejiwei
 * Date: 2021/2/27
 * Desc: 查询交易额接口以及返回参数处理
 * TODO 大屏展示的控制层
 * 接收客户端的请求Request，对请求进行处理，并给客户端响应Response
 *
 * @RestController = @ResponseBody + @Controller
 * 仅用Controller的话，方法的返回值，会认为是页面跳转；如果想返回一个jsonString的话，就不对了
 * 不做页面跳转 仅仅做String返回 那么就需要再加上 ResponseBody
 *
 * @RestController 可以加在方法上，
 *                  也可以放在类上，相当于指定了访问路径的命名空间
 *
 **/
@RequestMapping("/api/sugar")
@RestController
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    /*
    * 请求路径：/api/sugar/gmv
    *
    * 返回值类型：
       {
          "status": 0,
          "msg": "",
          "data": 1201001.961568421
        }
    *
    * */


    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {

        if (date == 0) {
            date = now();
        }

        BigDecimal gmv = productStatsService.getGMV(date);

        return "{" +
                "\"status\": 0," +
                "\"data\": " + gmv +
                "}";
    }

    private Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }

    @RequestMapping("/clickCt")
    public String m2() {
        return "success";
    }
}
