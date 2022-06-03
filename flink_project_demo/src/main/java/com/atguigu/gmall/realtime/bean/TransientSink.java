package com.atguigu.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Adam-Ma
 * @date 2022/6/3 18:19
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 用户标记不需要保存到 ClickHouse 中的注解
 *  四大元注解：
 *      @Target
 *      @Retention
 *      @Document
 *      @Inherit
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
