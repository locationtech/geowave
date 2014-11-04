package mil.nga.giat.geowave.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	GeoWaveBasicIT.class,
	GeoWaveMapReduceIT.class,
	GeoServerIT.class,
	GeoWaveServicesIT.class
})
public class GeoWaveITSuite
{
	@BeforeClass
	public static void setup() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP = true;
		}
		setupWfs();
		startServer();
	}
	@AfterClass
	public static void cleanup() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP = false;
			GeoWaveTestEnvironment.cleanup();
		}
	}
}
