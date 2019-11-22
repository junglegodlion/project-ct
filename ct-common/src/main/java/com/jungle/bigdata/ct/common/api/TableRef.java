package com.jungle.bigdata.ct.common.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//ElementType.TYPE当前注解只能用于类前
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME) //什么时候使用，这里是运行的时候使用
public @interface TableRef {
    String value();
}
