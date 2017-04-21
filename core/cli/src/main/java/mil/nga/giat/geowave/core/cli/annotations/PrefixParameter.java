package mil.nga.giat.geowave.core.cli.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the interface for a prefix parameter annotation
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD,
	ElementType.METHOD
})
public @interface PrefixParameter {
	/**
	 * Specify the prefix to apply to the prefix parameter
	 * 
	 * @return prefix to apply to the prefix parameter
	 */
	String prefix();
}
