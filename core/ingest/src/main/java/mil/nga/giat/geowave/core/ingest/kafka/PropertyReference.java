package mil.nga.giat.geowave.core.ingest.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This is just a hack to get access to the property name that we need to
 * overwrite in the kafka config property file.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD
})
public @interface PropertyReference {
	String value();
}
