package mil.nga.giat.geowave.mapreduce.splits;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.emory.mathcs.backport.java.util.Collections;

import java.util.Set;
import java.util.TreeMap;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.mapreduce.MapReduceMemoryDataStore;
import mil.nga.giat.geowave.mapreduce.MapReduceMemoryOperations;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class SplitsProviderTest
{

	@Test
	public void testQuantileSplits() throws Exception {
		
		//perform simple feature ingest
		MapReduceMemoryOperations mapReduceMemoryOps = new MapReduceMemoryOperations();
		MapReduceMemoryDataStore dataStore = new MapReduceMemoryDataStore(mapReduceMemoryOps);
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final PrimaryIndex idx = SimpleIngest.createSpatialIndex();
		final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
		
		final List<SimpleFeature> features = createFeatures(
				new SimpleFeatureBuilder(
						sft),
				8675309);
		
		try (IndexWriter<SimpleFeature> writer = dataStore.createWriter(
				fda,
				idx)) {
			for (final SimpleFeature feat : features) {
				writer.write(feat);
			}
		}
		
		//get splits and create reader for each RangeLocationPair, then summing up the rows for each split
		DistributableQuery query = new SpatialQuery(
				new GeometryFactory().toGeometry(
						new Envelope(
								-180, 
								180, 
								-90, 
								90)));
		QueryOptions queryOptions = new QueryOptions(fda.getAdapterId(), idx.getId());
		 
		List<InputSplit> splits = dataStore.getSplits(
				query, 
				queryOptions, 
				null, 
				null, 
				null, 
				null, 
				10, 
				Integer.MAX_VALUE);
				
		Map<Integer, Integer> actualDistribution = new TreeMap<Integer,Integer>();
		
		int totalCount = 0;
		int currentSplit = 1;
		
		for (InputSplit split : splits) {
			int countPerSplit = 0;	
			if (GeoWaveInputSplit.class.isAssignableFrom(split.getClass())) {
				GeoWaveInputSplit gwSplit = (GeoWaveInputSplit)split;
				for (ByteArrayId indexId : gwSplit.getIndexIds()) {
					SplitInfo splitInfo = gwSplit.getInfo(indexId);		
					for (RangeLocationPair p : splitInfo.getRangeLocationPairs()) {
						int countPerRange = 0;
						RecordReaderParams<?> readerParams = new RecordReaderParams(
								splitInfo.getIndex(), 
								dataStore.getAdapterStore(),
								Collections.singletonList(fda.getAdapterId()), 
								null, 
								null, 
								null, 
								splitInfo.isMixedVisibility(), 
								p.getRange(), 
								null, 
								GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER, 
								null);			
						try(Reader<?> reader = mapReduceMemoryOps.createReader(readerParams)){
							while(reader.hasNext()) {
								reader.next();
								countPerRange++;
							}
						}
						countPerSplit += countPerRange;
					}
				}
			}
			totalCount += countPerSplit;
			actualDistribution.put(currentSplit, countPerSplit);
			currentSplit++;
		}
		
		//calculate mean squared error for distribution
		double expected = 1.0/splits.size();
		double sum = 0;
		for (Map.Entry<Integer, Integer> entry : actualDistribution.entrySet()) {
			sum += Math.pow(((double)entry.getValue()/totalCount) - expected, 2);
		}
		double meanSquaredError = sum / splits.size();
		
		assertTrue(meanSquaredError < 0.01);
	}

	
	public static List<SimpleFeature> createFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final int firstFeatureId ) {

		int featureId = firstFeatureId;
		final List<SimpleFeature> feats = new ArrayList<>();
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
				feats.add(sft);
				featureId++;
			}
		}
		return feats;
	}
	
}


