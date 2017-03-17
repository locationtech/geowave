package mil.nga.giat.geowave.core.cli.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.TYPE
})
public @interface GeowaveOperation {
	public enum RestEnabledType {
		GET,
		POST,
		NONE
	}

	String name();

	RestEnabledType restEnabled() default RestEnabledType.NONE;

	Class<?> parentOperation() default Object.class;
}
