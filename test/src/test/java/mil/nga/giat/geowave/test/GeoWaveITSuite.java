package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.test.mapreduce.BasicMapReduceIT;
import mil.nga.giat.geowave.test.mapreduce.BulkIngestInputGenerationIT;
import mil.nga.giat.geowave.test.mapreduce.DBScanIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveKMeansIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveNNIT;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;
import mil.nga.giat.geowave.test.query.AttributesSubsetQueryIT;
import mil.nga.giat.geowave.test.query.SecondaryIndexingDriverIT;
import mil.nga.giat.geowave.test.query.SecondaryIndexingQueryIT;
import mil.nga.giat.geowave.test.service.GeoServerIT;
import mil.nga.giat.geowave.test.service.GeoWaveIngestGeoserverIT;
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
	GeoWaveRasterIT.class,
	BasicMapReduceIT.class,
	BulkIngestInputGenerationIT.class,
	// KDERasterResizeIT.class,
	GeoWaveKMeansIT.class,
	GeoWaveNNIT.class,
	GeoServerIT.class,
	GeoWaveServicesIT.class,
	GeoWaveIngestGeoserverIT.class,
	AttributesSubsetQueryIT.class,
	SecondaryIndexingDriverIT.class,
	SecondaryIndexingQueryIT.class,
	DBScanIT.class
})
public class GeoWaveITSuite
{
	@BeforeClass
	public static void setupSuite() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP.set(true);
		}
	}

	@AfterClass
	public static void cleanupSuite() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP.set(false);
			ServicesTestEnvironment.stopServices();
			MapReduceTestEnvironment.cleanupHdfsFiles();
			GeoWaveTestEnvironment.cleanup();
		}
	}
}