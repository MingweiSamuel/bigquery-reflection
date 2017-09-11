package win.pickban.maokai;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BqFieldPartition {
    String name() default "_pt";
    boolean key() default false;
    boolean autoTimestamp() default false;
    boolean include() default false;
}
