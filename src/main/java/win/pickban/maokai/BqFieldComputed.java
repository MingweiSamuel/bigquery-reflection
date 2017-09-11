package win.pickban.maokai;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BqFieldComputed {
    String expression() default "";
    String name() default "";
    String type() default "";
    boolean key() default false;
}
