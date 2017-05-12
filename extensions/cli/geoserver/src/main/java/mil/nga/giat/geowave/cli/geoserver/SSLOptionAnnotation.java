package mil.nga.giat.geowave.cli.geoserver;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation for specifying the base property name to
 *
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({
	FIELD,
	METHOD
})
public @interface SSLOptionAnnotation {
	String propertyBaseName();
}
