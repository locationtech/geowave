package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.test.mapreduce.GeoWavePDBSCANIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
//	GeoWaveBasicIT.class,
//	GeoWaveRasterIT.class,
//	BasicMapReduceIT.class,
	GeoWavePDBSCANIT.class,
//	GeoWaveKMeansIT.class,
	// KDEMapReduceIT.class, //for now this is commented out, further
	// investigation is required
	//GeoServerIT.class,
//	GeoWaveServicesIT.class
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
		//	ServicesTestEnvironment.stopServices();
			GeoWaveTestEnvironment.cleanup();
		}
	}
}
