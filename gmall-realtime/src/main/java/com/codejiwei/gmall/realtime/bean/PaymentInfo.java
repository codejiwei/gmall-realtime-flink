package com.codejiwei.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName PaymentInfo
 * @Author codejiwei
 * @Date 2021/2/21 17:25
 * @Version 1.0
 * @Description : 订单信息实体类
 **/
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}

