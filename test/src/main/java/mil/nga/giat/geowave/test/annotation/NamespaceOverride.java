package mil.nga.giat.geowave.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import mil.nga.giat.geowave.test.TestUtils;

@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD
})
public @interface NamespaceOverride {

	/**
	 * @return the namespace to associate the store with
	 */
	public String value() default TestUtils.TEST_NAMESPACE;
}
