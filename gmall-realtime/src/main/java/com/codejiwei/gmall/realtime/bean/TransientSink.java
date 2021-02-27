package com.codejiwei.gmall.realtime.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
/*
*   用该注解标记的属性，不需要插入到ClickHouse中
* */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
