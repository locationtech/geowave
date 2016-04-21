package mil.nga.giat.geowave.test.annotation;

import mil.nga.giat.geowave.test.TestUtils;

public @interface NamespaceOverride {

	/**
	 * @return the namespace to associate the store with
	 */
	public String value() default TestUtils.TEST_NAMESPACE;
}
