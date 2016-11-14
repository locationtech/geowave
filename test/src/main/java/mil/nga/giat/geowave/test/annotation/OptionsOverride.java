package mil.nga.giat.geowave.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD
})
public @interface OptionsOverride {

	/**
	 * @return a "key=value" pair that will override default options for the
	 *         client-side configuration of this datastore
	 */
	public String[] value() default "";
}
