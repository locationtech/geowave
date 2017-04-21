package mil.nga.giat.geowave.core.cli.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the interface for a GeoWave operation annotation
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.TYPE
})
public @interface GeowaveOperation {
	/**
	 * Specify the unique name identifier for the operation
	 * 
	 * @return unique name of the operation
	 */
	String name();

	/**
	 * Specify the parent operation that the operation falls under
	 * 
	 * @return Parent operation that the operation falls under
	 */
	Class<?> parentOperation() default Object.class;
}
