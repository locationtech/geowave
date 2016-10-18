package mil.nga.giat.geowave.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

import mil.nga.giat.geowave.test.config.ConfigCacheIT;
import mil.nga.giat.geowave.test.kafka.BasicKafkaIT;
import mil.nga.giat.geowave.test.landsat.LandsatIT;
import mil.nga.giat.geowave.test.mapreduce.BasicMapReduceIT;
import mil.nga.giat.geowave.test.mapreduce.BulkIngestInputGenerationIT;
import mil.nga.giat.geowave.test.mapreduce.DBScanIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveKMeansIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveNNIT;
import mil.nga.giat.geowave.test.mapreduce.KDERasterResizeIT;
import mil.nga.giat.geowave.test.query.AttributesSubsetQueryIT;
import mil.nga.giat.geowave.test.query.PolygonDataIdQueryIT;
import mil.nga.giat.geowave.test.query.SecondaryIndexingQueryIT;
import mil.nga.giat.geowave.test.query.SpatialTemporalQueryIT;
import mil.nga.giat.geowave.test.service.GeoServerIT;
import mil.nga.giat.geowave.test.service.GeoWaveIngestGeoserverIT;
import mil.nga.giat.geowave.test.service.GeoWaveServicesIT;
import mil.nga.giat.geowave.test.store.DataStatisticsStoreIT;

@RunWith(GeoWaveITSuiteRunner.class)
@SuiteClasses({
	GeoWaveBasicIT.class,
	BasicKafkaIT.class,
	BasicMapReduceIT.class,
	GeoWaveRasterIT.class,
	LandsatIT.class,
	BulkIngestInputGenerationIT.class,
	KDERasterResizeIT.class,
//	GeoWaveKMeansIT.class,
	GeoWaveNNIT.class,
	GeoServerIT.class,
	GeoWaveServicesIT.class,
	GeoWaveIngestGeoserverIT.class,
	AttributesSubsetQueryIT.class,
	SecondaryIndexingQueryIT.class,
	DBScanIT.class,
	SpatialTemporalQueryIT.class,
	PolygonDataIdQueryIT.class,
	ConfigCacheIT.class,
	DataStatisticsStoreIT.class,
})
public class GeoWaveITSuite
{
	@BeforeClass
	public static void setupSuite() {
		synchronized (GeoWaveITRunner.MUTEX) {
			GeoWaveITRunner.DEFER_CLEANUP.set(true);
		}
	}

	@AfterClass
	public static void tearDownSuite() {
		synchronized (GeoWaveITRunner.MUTEX) {
			GeoWaveITRunner.DEFER_CLEANUP.set(false);
		}
	}
}