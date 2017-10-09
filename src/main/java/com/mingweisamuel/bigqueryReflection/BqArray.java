package com.mingweisamuel.bigqueryReflection;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BqArray {
    String name() default "";
    Class<? extends Serializable> type() default Serializable.class;
}
