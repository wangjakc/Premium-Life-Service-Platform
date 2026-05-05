package com.hmdp.limter.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LimtRate {

    String key() default "rate_limit:";

    /**
     * 时间窗口大小（秒）
     */
    int window() default 10;

    /**
     * 时间窗口内允许的请求数
     */
    int limit() default 20;

    /**
     * 限流提示信息
     */
    String message() default "系统繁忙，请稍后再试";

    /**
     * 限流维度（默认按方法限流）
     */
    LimitType type() default LimitType.METHOD;

    enum LimitType {
        /**
         * 按调用方IP限流
         */
        IP,
        /**
         * 按用户ID限流
         */
        USER,
        /**
         * 按方法限流/全局限流（默认）
         */
        METHOD
    }
}
