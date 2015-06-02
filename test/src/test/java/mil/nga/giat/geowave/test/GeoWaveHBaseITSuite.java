package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.test.mapreduce.BulkIngestHBaseInputGenerationIT;
import mil.nga.giat.geowave.test.query.AttributesSubsetQueryHBaseIT;
import mil.nga.giat.geowave.test.service.ServicesTestEnvironment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	GeoWaveHBaseBasicIT.class,
	BulkIngestHBaseInputGenerationIT.class,
	AttributesSubsetQueryHBaseIT.class
})
public class GeoWaveHBaseITSuite
{
	@BeforeClass
	public static void setup() {
		synchronized (GeoWaveHBaseTestEnvironment.MUTEX) {
			GeoWaveHBaseTestEnvironment.DEFER_CLEANUP.set(true);
		}
	}

	@AfterClass
	public static void cleanup() {
		synchronized (GeoWaveHBaseTestEnvironment.MUTEX) {
			GeoWaveHBaseTestEnvironment.DEFER_CLEANUP.set(false);
			ServicesTestEnvironment.stopServices();
			GeoWaveHBaseTestEnvironment.cleanup();
		}
	}
}
