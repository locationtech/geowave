package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.test.mapreduce.BasicMapReduceIT;
import mil.nga.giat.geowave.test.service.GeoServerIT;
import mil.nga.giat.geowave.test.service.GeoWaveServicesIT;
import mil.nga.giat.geowave.test.service.ServicesTestEnvironment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	GeoWaveBasicIT.class,
	GeoWaveFeatureCollectionIT.class,
	GeoWaveRasterIT.class,
	BasicMapReduceIT.class,
	// KDEMapReduceIT.class, //for now this is commented out, further
	// investigation is required
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
	}

	@AfterClass
	public static void cleanup() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP = false;
			ServicesTestEnvironment.stopServices();
			GeoWaveTestEnvironment.cleanup();
		}
	}
}
