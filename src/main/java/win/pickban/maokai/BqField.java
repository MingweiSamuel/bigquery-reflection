package win.pickban.maokai;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BqField {
    String name() default "";
    String type() default "";
    boolean key() default false;
    boolean autoTimestamp() default false;
}