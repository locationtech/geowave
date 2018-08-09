package mil.nga.giat.geowave.mapreduce.splits;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.emory.mathcs.backport.java.util.Arrays;
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
//			GeoWaveStoreType.BIGTABLE,
//			GeoWaveStoreType.HBASE,
//			GeoWaveStoreType.DYNAMODB,
//			GeoWaveStoreType.CASSANDRA
	})
	protected DataStorePluginOptions dataStorePluginOptions;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
	
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
		public MapReduceMemoryDataStore mapReduceMemoryDataStore;
		public PrimaryIndex index;
		public GeotoolsFeatureDataAdapter adapter;
		public Distribution distribution;
		
		public DataStoreInfo(
				MapReduceMemoryDataStore mapReduceMemoryDataStore, 
				PrimaryIndex index,
				GeotoolsFeatureDataAdapter adapter, 
				Distribution distribution) {
			this.mapReduceMemoryDataStore = mapReduceMemoryDataStore;
			this.index = index;
			this.adapter = adapter;
			this.distribution = distribution;
		}
	}
	
	@BeforeClass
	public static void setup() {
		mapReduceMemoryOps = new MapReduceMemoryOperations();
		uniformDataStore = create(Distribution.UNIFORM);
		bimodalDataStore = create(Distribution.BIMODAL);
		skewedDataStore = create(Distribution.SKEWED);
	}
	
	@Test
	public void testUniform() {
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(helper(uniformDataStore, query, 10, 10) < 0.1);
	}
	
	@Test
	public void testBimodal() {
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(helper(bimodalDataStore, query, 10, 10) < 0.1);
	}
	
	@Test
	public void testSkewed() {
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(helper(skewedDataStore, query, 10, 10) < 0.1);
	}
	
	private static DataStoreInfo create(Distribution distr) {
		
		MapReduceMemoryDataStore dataStore = new MapReduceMemoryDataStore(
				mapReduceMemoryOps);
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final PrimaryIndex idx = SimpleIngest.createSpatialIndex();
		final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);

		try (IndexWriter<SimpleFeature> writer = dataStore.createWriter(
				fda,
				idx)) {
			
			switch(distr) {
				case UNIFORM:
					createUniformFeatures(
						new SimpleFeatureBuilder(
								sft),
						writer, 
						8675309);
				case BIMODAL:
					createBimodalFeatures(
						new SimpleFeatureBuilder(
								sft),
						writer,
						8675309);
				case SKEWED:
					createSkewedFeatures(
						new SimpleFeatureBuilder(
								sft),
						writer,
						8675309);
			}
			
			
		} 
		catch (MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					"", 
					e);
		} 
		catch (IOException e) {
			LOGGER.error(
					"", 
					e);
		}
		
		return new DataStoreInfo(
				dataStore, 
				idx, 
				fda,
				distr);
	}
	
	private double helper(DataStoreInfo dataStoreInfo, DistributableQuery query, int minSplits, int maxSplits) {

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
							System.out.println("old = " + countPerSplit +", new = ");
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

		System.out.println("case = " + dataStoreInfo.distribution);
		System.out.println("total = " + totalCount);
		double expected = 1.0 / splits.size();
		
		for (int i = 0; i < observed.length; i++) {
			observed[i] = observed[i] / totalCount;
		}
		double sum = 0;
		for (int i = 0; i < observed.length; i++) {
			sum += Math.pow(observed[i] - expected, 2);
		}
		double mse = sum / splits.size();
		System.out.println("mse = " + mse);
		
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
		for (int longitude = -180; longitude <= 0; longitude += 1) {
			if (longitude == -90) {
				continue;
			}
			for (int latitude = -180; latitude <= 0; latitude += (Math.abs(-90 - longitude) / 10)) {
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
}
