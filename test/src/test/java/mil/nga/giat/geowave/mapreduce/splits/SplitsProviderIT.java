package mil.nga.giat.geowave.mapreduce.splits;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.emory.mathcs.backport.java.util.Collections;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.mapreduce.MapReduceMemoryDataStore;
import mil.nga.giat.geowave.mapreduce.MapReduceMemoryOperations;
import mil.nga.giat.geowave.service.rest.GeoWaveOperationServiceWrapper;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveIT;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
public class SplitsProviderIT extends
	AbstractGeoWaveIT
{
	
	@GeoWaveTestStore(value = {
			GeoWaveStoreType.ACCUMULO,
			GeoWaveStoreType.BIGTABLE,
			GeoWaveStoreType.HBASE,
			GeoWaveStoreType.DYNAMODB,
			GeoWaveStoreType.CASSANDRA
	})
	protected DataStorePluginOptions dataStorePluginOptions;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
	private static long startMillis;
	private final static String testName = "SplitsProviderIT";
	
	private static MapReduceMemoryOperations mapReduceMemoryOps;
	private static DataStoreInfo uniformDataStore;
	private static DataStoreInfo bimodalDataStore;
	private static DataStoreInfo skewedDataStore;
	
	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStorePluginOptions;
	}
	
	enum Distribution{
		UNIFORM,
		BIMODAL,
		SKEWED
	}
	
	private static class DataStoreInfo{
		final public MapReduceMemoryDataStore mapReduceMemoryDataStore;
		final public PrimaryIndex index;
		final public GeotoolsFeatureDataAdapter adapter;
		
		public DataStoreInfo(
				MapReduceMemoryDataStore mapReduceMemoryDataStore, 
				PrimaryIndex index,
				GeotoolsFeatureDataAdapter adapter) {
			this.mapReduceMemoryDataStore = mapReduceMemoryDataStore;
			this.index = index;
			this.adapter = adapter;
		}
	}
	
	@BeforeClass
	public static void setup() {
		startMillis = System.currentTimeMillis();
		TestUtils.printStartOfTest(
				LOGGER,
				testName);
		
		mapReduceMemoryOps = new MapReduceMemoryOperations();
		uniformDataStore = createDataStore(Distribution.UNIFORM);
		bimodalDataStore = createDataStore(Distribution.BIMODAL);
		skewedDataStore = createDataStore(Distribution.SKEWED);
	}
	
	@AfterClass
	public static void reportTest() {
		TestUtils.printEndOfTest(
				LOGGER,
				testName,
				startMillis);
	}
	
	@Test
	public void testUniform() {
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(uniformDataStore, query, 12, 12) < 0.1);
	}
	
	@Test
	public void testBimodal() {
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(bimodalDataStore, query, 12, 12) < 0.1);
		
		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-120,
						-60,
						-90,
						90)));
		assertTrue(getSplitsMSE(bimodalDataStore, query, 12, 12) < 0.1);
		
		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-20,
						20,
						-90,
						90)));
		assertTrue(getSplitsMSE(bimodalDataStore, query, 12, 12) < 0.1);
	}
	
	@Test
	public void testSkewed() {
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(skewedDataStore, query, 12, 12) < 0.1);
		
		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						-140,
						-90,
						90)));
		assertTrue(getSplitsMSE(skewedDataStore, query, 12, 12) < 0.1);
		
		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						0,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(skewedDataStore, query, 12, 12) < 0.1);
	}
	
	private static DataStoreInfo createDataStore(Distribution distr) {
		
		final MapReduceMemoryDataStore dataStore = new MapReduceMemoryDataStore(
				mapReduceMemoryOps);
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final PrimaryIndex idx = SimpleIngest.createSpatialIndex();
		final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(
				sft);

		try (final IndexWriter<SimpleFeature> writer = dataStore.createWriter(
				fda,
				idx)) {
			
			switch(distr) {
				case UNIFORM:
					createUniformFeatures(
						new SimpleFeatureBuilder(
								sft),
						writer, 
						100000);
					break;
				case BIMODAL:
					createBimodalFeatures(
						new SimpleFeatureBuilder(
								sft),
						writer,
						400000);
					break;
				case SKEWED:
					createSkewedFeatures(
						new SimpleFeatureBuilder(
								sft),
						writer,
						700000);
					break;
				default:
					LOGGER.error("Invalid Distribution");
					throw new Exception();
			}
		} 
		catch (MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					"MismathcedIndexToAdapterMapping exception thrown when creating data store writer", 
					e);
		} 
		catch (IOException e) {
			LOGGER.error(
					"IOException thrown when creating data store writer", 
					e);
		} catch (Exception e) {
			LOGGER.error(
					"Exception thrown when creating data store writer", 
					e);
		}
		
		return new DataStoreInfo(
				dataStore, 
				idx, 
				fda);
	}
	
	private double getSplitsMSE(DataStoreInfo dataStoreInfo, DistributableQuery query, int minSplits, int maxSplits) {

		// get splits and create reader for each RangeLocationPair, then summing
		// up the rows for each split
		
		QueryOptions queryOptions = new QueryOptions(
				dataStoreInfo.adapter.getAdapterId(),
				dataStoreInfo.index.getId());

		List<InputSplit> splits = null;
		try {
			splits = dataStoreInfo.mapReduceMemoryDataStore.getSplits(
					query,
					queryOptions,
					null,
					null,
					null,
					null,
					minSplits,
					maxSplits);
		} 
		catch (IOException e) {
			LOGGER.error(
					"IOException thrown when calling getSplits", 
					e);
		} 
		catch (InterruptedException e) {
			LOGGER.error(
					"InterruptedException thrown when calling getSplits", 
					e);
		}

		double[] observed = new double[splits.size()];
		
		int totalCount = 0;
		int currentSplit = 0;

		for (InputSplit split : splits) {
			int countPerSplit = 0;
			if (GeoWaveInputSplit.class.isAssignableFrom(split.getClass())) {
				GeoWaveInputSplit gwSplit = (GeoWaveInputSplit) split;
				for (ByteArrayId indexId : gwSplit.getIndexIds()) {
					SplitInfo splitInfo = gwSplit.getInfo(indexId);
					for (RangeLocationPair p : splitInfo.getRangeLocationPairs()) {
						RecordReaderParams<?> readerParams = new RecordReaderParams(
								splitInfo.getIndex(),
								dataStoreInfo.mapReduceMemoryDataStore.getAdapterStore(),
								Collections.singletonList(dataStoreInfo.adapter.getAdapterId()),
								null,
								null,
								null,
								splitInfo.isMixedVisibility(),
								p.getRange(),
								null,
								GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
								null);
						try (Reader<?> reader = mapReduceMemoryOps.createReader(readerParams)) {
							while (reader.hasNext()) {
								reader.next();
								countPerSplit++;
	
							}
						} 
						catch (Exception e) {
							LOGGER.error(
									"Exception thrown when calling createReader", 
									e);
						}		
					}
				}
			}
			totalCount += countPerSplit;
			observed[currentSplit] = countPerSplit;
			currentSplit++;
		}

		double expected = 1.0 / splits.size();

		double sum = 0;
		
		for (int i = 0; i < observed.length; i++) {
			sum += Math.pow((observed[i] / totalCount) - expected, 2);
		}
		
		return sum / splits.size();
	}

	public static void createUniformFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final IndexWriter<SimpleFeature> writer,
			final int firstFeatureId) {

		int featureId = firstFeatureId;
		for (int longitude = -180; longitude <= 180; longitude += 1) {
			for (int latitude = -90; latitude <= 90; latitude += 1) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
	}
	
	public static void createBimodalFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final IndexWriter<SimpleFeature> writer,
			final int firstFeatureId) {

		int featureId = firstFeatureId;
		for (double longitude = -180.0; longitude <= 0.0; longitude += 1.0) {
			if (longitude == -90) {
				continue;
			}
			for (double latitude = -180.0; latitude <= 0.0; latitude += (Math.abs(-90.0 - longitude) / 10.0)) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
		
		for (double longitude = 0.0; longitude <= 180.0; longitude += 1.0) {
			if (longitude == 90) {
				continue;
			}
			for (double latitude = 0.0; latitude <= 180.0; latitude += (Math.abs(90.0 - longitude) / 10.0)) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
	}
	
	public static void createSkewedFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final IndexWriter<SimpleFeature> writer,
			final int firstFeatureId) {

		int featureId = firstFeatureId;
		for (double longitude = -180.0; longitude <= 180.0; longitude += 1.0) {
			for (double latitude = -90.0; latitude <= 90.0; latitude += ((longitude + 181.0) / 10.0)) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
	}
}
