package com.codejiwei.gmall.realtime.app.func;

import com.codejiwei.gmall.realtime.common.GmallConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * Author: codejiwei
 * Date: 2021/2/26
 * Desc: 自定义商品行为关键词UDTF
 **/
@FunctionHint(output = @DataTypeHint("ROW<ct BIGINT, source STRING>"))
public class KeywordProductUDTF  extends TableFunction<Row> {
    public void eval(Long clickCt, Long cartCt, Long orderCt) {
        if (clickCt > 0L) {
            Row rowClick = new Row(2);
            rowClick.setField(0, clickCt);
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            collect(rowClick);
        }
        if (cartCt > 0L) {
            //创建一个一行对象，这一行有两个参数：加购数，CART
            Row rowCart = new Row(2);
            rowCart.setField(0, cartCt);
            rowCart.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowCart);
        }
        if (orderCt > 0L) {
            Row rowOrder = new Row(2);
            rowOrder.setField(0, orderCt);
            rowOrder.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowOrder);
        }

    }
}
