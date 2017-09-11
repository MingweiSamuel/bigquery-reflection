package com.mingweisamuel.bigqueryReflection;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BqDescription {
    String value() default "";
    /** True if changing a description should trigger an update. */
    boolean triggerUpdate() default false;
}
