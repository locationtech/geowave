package mil.nga.giat.geowave.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import mil.nga.giat.geowave.test.AccumuloStoreTestEnvironment;
import mil.nga.giat.geowave.test.HBaseStoreTestEnvironment;
import mil.nga.giat.geowave.test.StoreTestEnvironment;
import mil.nga.giat.geowave.test.TestUtils;

/**
 * The <code>DataStores</code> annotation specifies the GeoWave DataStore to be
 * run when a class annotated with <code>@RunWith(GeoWaveIT.class)</code> is
 * run.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD,
	ElementType.TYPE
})
public @interface GeoWaveTestStore {
	/**
	 * @return the data stores to run with
	 */
	public GeoWaveStoreType[] value();

	/**
	 * @return the namespace to associate the store with
	 */
	public String namespace() default TestUtils.TEST_NAMESPACE;

	public static enum GeoWaveStoreType {
		ACCUMULO(
				AccumuloStoreTestEnvironment.getInstance()),
		HBASE(
				HBaseStoreTestEnvironment.getInstance());
		// HBASE(
		// HBaseStoreTestEnvironment.getInstance());
		private final StoreTestEnvironment testEnvironment;

		private GeoWaveStoreType(
				final StoreTestEnvironment testEnvironment ) {
			this.testEnvironment = testEnvironment;
		}

		public StoreTestEnvironment getTestEnvironment() {
			return testEnvironment;
		}

	}
}
