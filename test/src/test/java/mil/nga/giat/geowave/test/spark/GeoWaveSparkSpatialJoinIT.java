package mil.nga.giat.geowave.test.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.Envelope2D;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Iterators;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import scala.Console;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.analytic.spark.sparksql.SimpleFeatureDataFrame;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunctionRegistry;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomIntersects;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomWriter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.datastore.hbase.cli.HBaseMiniCluster;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

class PrettyPrintingMap<K, V> {
    private Map<K, V> map;

    public PrettyPrintingMap(Map<K, V> map) {
        this.map = map;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Entry<K, V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<K, V> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        return sb.toString();

    }
}

class SpatialJoin implements Serializable {
	private SparkSession session;
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);
	public JavaPairRDD<GeoWaveInputKey, String> joinResults = null;
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> finalResults = null;

	public SpatialJoin(SparkSession spark) {
		this.session = spark;
	}
	
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getResults() {
		//TODO: Implement
		return null;
	}
	
	public void performJoin(
			SparkContext sc,
			DataStorePluginOptions leftStore,
			ByteArrayId leftAdapterId,
			DataStorePluginOptions rightStore,
			ByteArrayId rightAdapterId,
			GeomFunction predicate) throws IOException {
		
		SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
		PrimaryIndex index = provider.createPrimaryIndex();
		TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();

		DataAdapter<?> leftAdapter = leftStore.createAdapterStore().getAdapter(leftAdapterId);
		DataAdapter<?> rightAdapter = rightStore.createAdapterStore().getAdapter(rightAdapterId);

		JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD = GeoWaveRDD.rddForSimpleFeatures(
				sc, 
				leftStore, 
				null, 
				new QueryOptions(leftAdapter));
		
		
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD = GeoWaveRDD.rddForSimpleFeatures(
				sc, 
				rightStore, 
				null, 
				new QueryOptions(rightAdapter));

		//Generate Index RDDs for each set of data.
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftIndex = this.indexData(leftRDD);
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightIndex = this.indexData(rightRDD);
		
		
		long leftCount = leftIndex.count();
		long rightCount = rightIndex.count();
		
		if(leftCount == 0 || rightCount == 0) {
			LOGGER.error("No features for one index");
			return;
		}
		
		SubStrategy[] tierStrategies = strategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		byte minTierId = (byte) 0;
		byte maxTierId = (byte)(tierCount - 1);
		
		//TODO: PROFILE!
		//Find the range of data in right set (worst case: min to max tier) (best case: single tier)
		//Still requires a check within loop, but sets a lower/upper bound. May not be a performance boost (profile)
		byte rightMaxDataTierId = this.findFirstDataTier(rightIndex, maxTierId, false);
		byte rightMinDataTierId = this.findFirstDataTier(rightIndex, minTierId, true);

		//Iterate through tiers and join each tier containing data from each set.
		for(int iTier = maxTierId; iTier >= minTierId; iTier--) {
			SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) tierStrategies[iTier].getIndexStrategy();
			byte leftTierId = tierStrategy.tier;
			
			//Filter left feature set for tier
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftTier = this.filterTier(leftIndex, leftTierId);
			
			//leftTier.cache();
			
			//If we have no data on the left side at this tier just continue the loop
			long leftCountTier = leftTier.count();
			if (leftCountTier == 0) {
				continue;
			}
			

			//Loop through the range of tiers that may contain data on the right side
			for( byte rightTierId = rightMaxDataTierId; rightTierId >= rightMinDataTierId; rightTierId-- ) {
				//Filter the tier from right dataset
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightTier = this.filterTier(rightIndex, rightTierId);
				
				long rightCountTier = rightTier.count();
				if (rightCountTier == 0) {
					continue;
				}
				
				//We found a tier on the right with geometry to test against.
				//Reproject one of the data sets to the coarser tier
				if(leftTierId > rightTierId) {
					leftTier = this.reprojectToTier(leftTier, rightTierId);
				} else if (leftTierId < rightTierId) {
					rightTier = this.reprojectToTier(rightTier, leftTierId);
				}

				//Once we have each tier and index at same resolution then join and compare each of the sets
				JavaPairRDD<GeoWaveInputKey, String> finalTierMatches = this.joinAndCompareTiers(leftTier,rightTier, predicate);
				
				long finalTierCount = finalTierMatches.count();
				LOGGER.warn("Left Tier: " + leftTierId + " Right Tier: " + rightTierId + " Match Count= " + finalTierCount );
				//Combine each tier into a final list of matches for all tiers
				if(this.joinResults == null) {
					this.joinResults = finalTierMatches;
				} else {
					this.joinResults = this.joinResults.union(finalTierMatches);
				}
			}

		}
		this.joinResults = this.joinResults.distinct();
		
		this.outputResultsToDataStore(leftStore, leftAdapterId, leftRDD, rightRDD);
	}
	
	private void outputResultsToDataStore(DataStorePluginOptions outputStore,
			ByteArrayId outputId,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD) {
		
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined = this.joinResults.join(leftRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) );
		
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined = this.joinResults.join(rightRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) );
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> joinedRDD = leftJoined.union(rightJoined);
		
		//SimpleFeatureDataFrame dataframe = new SimpleFeatureDataFrame(this.session);
		//dataframe.init(outputStore, outputId);
		this.finalResults = joinedRDD;
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexData(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> data)
	{
		//Flat map is used because each pair can potentially yield 1+ output rows within rdd.
		//Instead of storing whole feature on index maybe just output Key + Bounds
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexedData = data.flatMapToPair(new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>,ByteArrayId, Tuple2<GeoWaveInputKey,String>>() {
			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> call(
					Tuple2<GeoWaveInputKey, SimpleFeature> t )
					throws Exception {
				
				//Flattened output array.
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> result = new ArrayList<>();
				
				//TODO: This gets made on every node yet it should/could be shared because it defines the space
				//and index strategy. Figure out how to efficiently share this across workers.
				SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
				PrimaryIndex index = provider.createPrimaryIndex();
				NumericIndexStrategy strategy = index.getIndexStrategy();
				
				//Pull feature to index from tuple
				SimpleFeature inputFeature = t._2;
				
				GeomWriter writer = new GeomWriter();
				Geometry geom = (Geometry)inputFeature.getDefaultGeometry();
				if(geom == null) {
					LOGGER.warn("Null Geometry Found");
					return result.iterator();
				}
				String geomString = writer.write(geom);
				
				//Extract bounding box from input feature
				BoundingBox bounds = inputFeature.getBounds();
				NumericRange xRange = new NumericRange(bounds.getMinX(), bounds.getMaxX());
				NumericRange yRange = new NumericRange(bounds.getMinY(), bounds.getMaxY());
				
				if(bounds.isEmpty()) {
					Envelope internalEnvelope = geom.getEnvelopeInternal();
					xRange = new NumericRange(internalEnvelope.getMinX(), internalEnvelope.getMaxX());
					yRange = new NumericRange(internalEnvelope.getMinY(), internalEnvelope.getMaxY());
				
				}
				NumericData[] boundsRange = {
					xRange,
					yRange	
				};
				
				//Convert the data to how the api expects and index using strategy above
				BasicNumericDataset convertedBounds = new BasicNumericDataset(boundsRange);
				List<ByteArrayId> insertIds = strategy.getInsertionIds(convertedBounds);
				
				//Sometimes the result can span more than one row/cell of a tier
				//When we span more than one row each individual get added as a separate output pair
				for(Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
					ByteArrayId id = iter.next();
					//Id decomposes to byte array of Tier, Bin, SFC (Hilbert in this case) id)
					//There may be value in decomposing the id and storing tier + sfcIndex as a tuple key of new RDD
					Tuple2<GeoWaveInputKey, String> valuePair = new Tuple2<>(t._1, geomString);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>(id, valuePair );
					result.add(indexPair);
				}
				
				return result.iterator();
			}
			
		});		
		return indexedData;
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> filterTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexRDD, byte tierId) {
		return indexRDD.filter(new Function<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>, Boolean>() {

			@Override
			public Boolean call(
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> v1 )
					throws Exception {
				ByteArrayId rowId = v1._1();
				if(tierId == rowId.getBytes()[0]) {
					return true;
				}
				return false;
			}
			
		});
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> reprojectToTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> tierIndex, byte targetTierId) {
		return tierIndex.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<GeoWaveInputKey, String>>,ByteArrayId,Tuple2<GeoWaveInputKey, String>>() {

			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> call(
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> t )
					throws Exception {
				
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> reprojected = new ArrayList<>();
				//TODO: This gets made on every node yet it should/could be shared because it defines the space
				//and index strategy. Figure out how to efficiently share this across workers.
				SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
				PrimaryIndex index = provider.createPrimaryIndex();
				TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();
				SubStrategy[] strats = strategy.getSubStrategies();
				
				if(strategy.tierExists(targetTierId) == false) {
					LOGGER.warn("Tier does not exist in strategy!");
					return reprojected.iterator();
				}
				
				int stratCount = strats.length;
				SingleTierSubStrategy targetStrategy = null;
				for(int i = 0; i < stratCount; i++) {
					SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) strats[i].getIndexStrategy();
					if(tierStrategy.tier == targetTierId) {
						targetStrategy = tierStrategy;
						break;
					}
				}
		
				//Pull feature to index from tuple
				GeomReader gReader = new GeomReader();
				
				//Parse geom from string
				Geometry geom = gReader.read(t._2._2());
				NumericRange xRange = new NumericRange(geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMaxX());
				NumericRange yRange = new NumericRange(geom.getEnvelopeInternal().getMinY(), geom.getEnvelopeInternal().getMaxY());
				NumericData[] boundsRange = {
					xRange,
					yRange	
				};
				
				//Convert the data to how the api expects and index using strategy above
				BasicNumericDataset convertedBounds = new BasicNumericDataset(boundsRange);
				List<ByteArrayId> insertIds = targetStrategy.getInsertionIds(convertedBounds);
				
				//When we span more than one row each individual get added as a separate output pair
				for(Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
					ByteArrayId id = iter.next();
					//Id decomposes to byte array of Tier, Bin, SFC (Hilbert in this case) id)
					//There may be value in decomposing the id and storing tier + sfcIndex as a tuple key of new RDD
					Tuple2<GeoWaveInputKey, String> valuePair = new Tuple2<>(t._2._1, t._2._2);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>(id, valuePair);
					reprojected.add(indexPair);
				}
				
				return reprojected.iterator();
			}
			
		});
	}
	
	private JavaPairRDD<GeoWaveInputKey, String> joinAndCompareTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftTier,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightTier,
			GeomFunction predicate) {
		//Cogroup looks at each RDD and grab keys that are the same in this case ByteArrayId
		//Can we rely that cogroup will filter ByteArrayId class correctly?
		JavaPairRDD<GeoWaveInputKey, String> finalMatches = null;
		JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>, Iterable<Tuple2<GeoWaveInputKey, String>>>> joinedTiers = leftTier.cogroup(rightTier);
		//We need to go through the pairs and test each feature against each other
		//End with a combined RDD for that tier.
		long groupedCount = joinedTiers.count();
		LOGGER.warn("Cogroup result count = " + groupedCount);
		finalMatches = joinedTiers.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>,Iterable<Tuple2<GeoWaveInputKey, String>>>>, GeoWaveInputKey, String>() {

			@Override
			public Iterator<Tuple2<GeoWaveInputKey, String>> call(
					Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>, Iterable<Tuple2<GeoWaveInputKey, String>>>> t )
					throws Exception {
				List<Tuple2<GeoWaveInputKey, String>> resultPairs = new ArrayList<>();
				
				Iterable<Tuple2<GeoWaveInputKey, String>> leftFeatures = t._2._1();
				Iterable<Tuple2<GeoWaveInputKey, String>> rightFeatures = t._2._2();
				
				Iterator<Tuple2<GeoWaveInputKey, String>> leftIter = leftFeatures.iterator();
				Iterator<Tuple2<GeoWaveInputKey, String>> rightIter = rightFeatures.iterator();
				int countLeft = Iterators.size(leftIter);
				int countRight = Iterators.size(rightIter);

				
				//Early out if there are none for a given key on either side.
				if(countLeft == 0 || countRight == 0) {
					return resultPairs.iterator();
				}
				
				GeoWaveInputKey[] leftIds = new GeoWaveInputKey[countLeft];
				String[] leftGeom = new String[countLeft];
				
				GeoWaveInputKey[] rightIds = new GeoWaveInputKey[countRight];
				String[] rightGeom = new String[countRight];

				//Reset iterator
				int iLeft = 0;
				leftIter = leftFeatures.iterator();
				while(leftIter.hasNext()) {
					Tuple2<GeoWaveInputKey, String> feature = leftIter.next();
					leftIds[iLeft] = feature._1();
					leftGeom[iLeft++] = feature._2();
				}
				
				int iRight = 0;
				rightIter = rightFeatures.iterator();
				while(rightIter.hasNext()) {
					Tuple2<GeoWaveInputKey, String> feature = rightIter.next();
					rightIds[iRight] = feature._1();
					rightGeom[iRight++] = feature._2();
				}
				
				//Compare each filtered set against one another and add feature pairs that
				for(int left = 0; left < countLeft; left++) {
					String leftFeature = leftGeom[left];
					
					for(int right = 0; right < countRight; right++) {
						String rightFeature = rightGeom[right];
						
						if(predicate.call(leftFeature, rightFeature)) {
							Tuple2<GeoWaveInputKey,String> leftPair = new Tuple2<>(leftIds[left],leftFeature);
							Tuple2<GeoWaveInputKey,String> rightPair = new Tuple2<>(rightIds[right],rightFeature);
							resultPairs.add(leftPair);
							resultPairs.add(rightPair);
						}
					}
				}
				
				return resultPairs.iterator();
			}
			
		});

		//Filter distinct id/geom pairs from matched results.
		finalMatches = finalMatches.distinct();
		
		return finalMatches;
	}
	
	private byte findFirstDataTier(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightIndex,
			byte startTierId, 
			boolean increment) {
		byte returnTierId = startTierId;
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightTier = this.filterTier(rightIndex, startTierId);

		long rightCountTier = rightTier.count();
		while(rightCountTier == 0) {
			if(increment == true) {
				returnTierId += 1;
			} else {
				returnTierId -= 1;
			}
			rightTier = this.filterTier(rightIndex, returnTierId);
			rightCountTier = rightTier.count();
		}
		return returnTierId;
	}
	
}



@RunWith(GeoWaveITRunner.class)
public class GeoWaveSparkSpatialJoinIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-filter.shp";

	private static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-filter.shp";

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String CQL_DELETE_STR = "STATE = 'TX'";
	
	@GeoWaveTestStore(value = {
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveSparkSpatialJoinIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
		
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveSparkSpatialJoinIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}
	
	public void standupHBase() {
		String[] args = new String[2];
		args[0] = "interactive";
		args[1] = "false";
		try {
			HBaseMiniCluster.main(args);
		}
		catch (Exception e) {
			LOGGER.error("Unable to standup HBase for test.");
			e.printStackTrace();
		}
	}

	@Test
	public void testSpatialJoin() {
		
		// Set up Spark
		SparkSession spark = SparkSession
				.builder()
				.appName("SpatialJoinTest")
				.master("local")
				.config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
				.config("spark.kryo.registrator", "mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator")
				.getOrCreate();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		GeomFunctionRegistry.registerGeometryFunctions(spark);

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());
		long mark = System.currentTimeMillis();
		
		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + 1 + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + 1 + " thread(s).");
		
		SpatialJoin join = new SpatialJoin(spark);
		ByteArrayId hail_adapter = new ByteArrayId("hail");
		ByteArrayId tornado_adapter = new ByteArrayId("tornado_tracks");
		GeomIntersects predicate = new GeomIntersects();

		mark = System.currentTimeMillis();

		long finalCount = 0;
		try {
			join.performJoin(sc.sc(), dataStore, hail_adapter, dataStore, tornado_adapter, predicate);

			finalCount = join.finalResults.count();
		}
		catch (IOException e) {
			LOGGER.error("Could not perform join");
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			sc.close();
			Assert.fail();
		}

		long indexJoinDur = (System.currentTimeMillis() - mark);
		

		long bruteCount = 0;
		mark = System.currentTimeMillis();
		try {
			SimpleFeatureDataFrame hailFrame = new SimpleFeatureDataFrame(spark);
			SimpleFeatureDataFrame tornadoFrame = new SimpleFeatureDataFrame(spark);
			DataAdapter<?> hailAdapter = dataStore.createAdapterStore().getAdapter(hail_adapter);
			DataAdapter<?> tornadoAdapter = dataStore.createAdapterStore().getAdapter(tornado_adapter);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> hailRDD = GeoWaveRDD.rddForSimpleFeatures(
					sc.sc(), 
					dataStore, 
					null, 
					new QueryOptions(hailAdapter));
			hailFrame.init(dataStore, hail_adapter);
			
			hailFrame.getDataFrame(hailRDD).createOrReplaceTempView("hail");
			
			
			
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> tornadoRDD = GeoWaveRDD.rddForSimpleFeatures(
					sc.sc(), 
					dataStore, 
					null, 
					new QueryOptions(tornadoAdapter));
			tornadoFrame.init(dataStore, tornado_adapter);

			tornadoFrame.getDataFrame(tornadoRDD).createOrReplaceTempView("tornado");
			
			Dataset<Row> result = spark.sql("select * from hail, tornado where geomIntersects(hail.geom,tornado.geom)");
			
			result = result.dropDuplicates();
			bruteCount = result.count();
			result.show(50);
			
		}
		catch (IOException e) {
			LOGGER.error("Could not load hail dataset into RDD");
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			sc.close();
			Assert.fail();
		}
		dur = (System.currentTimeMillis() - mark);
		
		LOGGER.warn("Indexed Joined Count= " + finalCount);
		LOGGER.warn("Brute force join count= " + bruteCount);
		LOGGER.warn("Indexed Join duration = " + indexJoinDur + " ms.");
		LOGGER.warn("Brute Join duration = " + dur + " ms.");
		

		

		TestUtils.deleteAll(dataStore);
		sc.close();
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}