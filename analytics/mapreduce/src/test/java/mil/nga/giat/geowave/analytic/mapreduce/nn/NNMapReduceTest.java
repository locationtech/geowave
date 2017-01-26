package mil.nga.giat.geowave.analytic.mapreduce.nn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureWritable;
import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.kmeans.SimpleFeatureImplSerialization;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class NNMapReduceTest
{

	MapDriver<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable> mapDriver;
	ReduceDriver<PartitionDataWritable, AdapterWithObjectWritable, Text, Text> reduceDriver;
	SimpleFeatureType ftype;
	final GeometryFactory factory = new GeometryFactory();

	@Before
	public void setUp()
			throws IOException {
		final NNMapReduce.NNMapper<SimpleFeature> nnMapper = new NNMapReduce.NNMapper<SimpleFeature>();
		final NNMapReduce.NNReducer<SimpleFeature, Text, Text, Boolean> nnReducer = new NNMapReduce.NNSimpleFeatureIDOutputReducer();

		mapDriver = MapDriver.newMapDriver(nnMapper);
		reduceDriver = ReduceDriver.newReduceDriver(nnReducer);

		mapDriver.getConfiguration().set(
				GeoWaveConfiguratorBase.enumToConfKey(
						NNMapReduce.class,
						PartitionParameters.Partition.DISTANCE_THRESHOLDS),
				"0.0002,0.0002");

		reduceDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						NNMapReduce.class,
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS),
				FeatureCentroidOrthodromicDistanceFn.class,
				DistanceFn.class);
		reduceDriver.getConfiguration().setDouble(
				GeoWaveConfiguratorBase.enumToConfKey(
						NNMapReduce.class,
						PartitionParameters.Partition.MAX_DISTANCE),
				0.001);

		ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();

		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				new FeatureDataAdapter(
						ftype));

		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				new FeatureDataAdapter(
						ftype));

		serializations();
	}

	private SimpleFeature createTestFeature(
			final Coordinate coord ) {
		return AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				UUID.randomUUID().toString(),
				"fred",
				"NA",
				20.30203,
				factory.createPoint(coord),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

	}

	private void serializations() {
		final String[] strings = reduceDriver.getConfiguration().getStrings(
				"io.serializations");
		final String[] newStrings = new String[strings.length + 1];
		System.arraycopy(
				strings,
				0,
				newStrings,
				0,
				strings.length);
		newStrings[newStrings.length - 1] = SimpleFeatureImplSerialization.class.getName();
		reduceDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);

		mapDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);
	}

	@Test
	public void testMapper()
			throws IOException {

		final SimpleFeature feature1 = createTestFeature(new Coordinate(
				30.0,
				30.00000001));
		final SimpleFeature feature2 = createTestFeature(new Coordinate(
				179.9999999999,
				30.0000001));
		final SimpleFeature feature3 = createTestFeature(new Coordinate(
				30.00000001,
				30.00000001));
		final SimpleFeature feature4 = createTestFeature(new Coordinate(
				-179.9999999999,
				30.0000001));

		final GeoWaveInputKey inputKey1 = new GeoWaveInputKey();
		inputKey1.setAdapterId(new ByteArrayId(
				ftype.getTypeName()));
		inputKey1.setDataId(new ByteArrayId(
				feature1.getID()));

		final GeoWaveInputKey inputKey2 = new GeoWaveInputKey();
		inputKey2.setAdapterId(new ByteArrayId(
				ftype.getTypeName()));
		inputKey2.setDataId(new ByteArrayId(
				feature2.getID()));

		final GeoWaveInputKey inputKey3 = new GeoWaveInputKey();
		inputKey3.setAdapterId(new ByteArrayId(
				ftype.getTypeName()));
		inputKey3.setDataId(new ByteArrayId(
				feature4.getID()));

		final GeoWaveInputKey inputKey4 = new GeoWaveInputKey();
		inputKey4.setAdapterId(new ByteArrayId(
				ftype.getTypeName()));
		inputKey4.setDataId(new ByteArrayId(
				feature4.getID()));

		mapDriver.addInput(
				inputKey1,
				feature1);
		mapDriver.addInput(
				inputKey2,
				feature2);
		mapDriver.addInput(
				inputKey3,
				feature3);
		mapDriver.addInput(
				inputKey4,
				feature4);

		final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults = mapDriver.run();
		assertEquals(
				10, // includes overlap
				mapperResults.size());
		assertFalse(getPartitionDataFor(
				mapperResults,
				feature1.getID(),
				true).isEmpty());
		assertFalse(getPartitionDataFor(
				mapperResults,
				feature2.getID(),
				true).isEmpty());
		assertFalse(getPartitionDataFor(
				mapperResults,
				feature2.getID(),
				false).isEmpty());
		assertFalse(getPartitionDataFor(
				mapperResults,
				feature3.getID(),
				true).isEmpty());

		assertTrue(intersects(
				getPartitionDataFor(
						mapperResults,
						feature1.getID(),
						true),
				getPartitionDataFor(
						mapperResults,
						feature3.getID(),
						true)));

		assertTrue(intersects(
				getPartitionDataFor(
						mapperResults,
						feature2.getID(),
						false),
				getPartitionDataFor(
						mapperResults,
						feature4.getID(),
						false)));

		final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> partitions = getReducerDataFromMapperInput(mapperResults);
		assertEquals(
				3,
				partitions.size());

		reduceDriver.addAll(partitions);

		final List<Pair<Text, Text>> reduceResults = reduceDriver.run();

		assertEquals(
				4,
				reduceResults.size());

		assertEquals(
				feature3.getID(),
				find(
						reduceResults,
						feature1.getID()).toString());

		assertEquals(
				feature1.getID(),
				find(
						reduceResults,
						feature3.getID()).toString());

		assertEquals(
				feature4.getID(),
				find(
						reduceResults,
						feature2.getID()).toString());

		assertEquals(
				feature2.getID(),
				find(
						reduceResults,
						feature4.getID()).toString());
	}

	@Test
	public void testWritable()
			throws IOException {

		final PartitionDataWritable writable1 = new PartitionDataWritable();
		final PartitionDataWritable writable2 = new PartitionDataWritable();

		writable1.setPartitionData(new PartitionData(
				new ByteArrayId(
						"abc"),
				true));
		writable2.setPartitionData(new PartitionData(
				new ByteArrayId(
						"abc"),
				false));

		assertTrue(writable1.compareTo(writable2) == 0);
		writable2.setPartitionData(new PartitionData(
				new ByteArrayId(
						"abd"),
				false));
		assertTrue(writable1.compareTo(writable2) < 0);
		writable2.setPartitionData(new PartitionData(
				new ByteArrayId(
						"abd"),
				true));
		assertTrue(writable1.compareTo(writable2) < 0);

		final DataOutputByteBuffer output = new DataOutputByteBuffer();
		writable1.write(output);
		output.flush();
		final DataInputByteBuffer input = new DataInputByteBuffer();
		input.reset(output.getData());

		writable2.readFields(input);
		assertTrue(writable1.compareTo(writable2) == 0);

	}

	private Text find(
			final List<Pair<Text, Text>> outputSet,
			final String key ) {
		for (final Pair<Text, Text> item : outputSet) {
			if (key.equals(item.getFirst().toString())) {
				return item.getSecond();
			}
		}
		return null;

	}

	private List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> getReducerDataFromMapperInput(
			final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults ) {
		final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> reducerInputSet = new ArrayList<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>>();
		for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
			getListFor(
					pair.getFirst(),
					reducerInputSet).add(
					pair.getSecond());

		}
		return reducerInputSet;
	}

	private List<AdapterWithObjectWritable> getListFor(
			final PartitionDataWritable pd,
			final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> reducerInputSet ) {
		for (final Pair<PartitionDataWritable, List<AdapterWithObjectWritable>> pair : reducerInputSet) {
			if (pair.getFirst().compareTo(
					pd) == 0) {
				return pair.getSecond();
			}
		}
		final List<AdapterWithObjectWritable> newPairList = new ArrayList<AdapterWithObjectWritable>();
		reducerInputSet.add(new Pair(
				pd,
				newPairList));
		return newPairList;
	}

	private boolean intersects(
			final List<PartitionData> setOne,
			final List<PartitionData> setTwo ) {
		for (final PartitionData pdOne : setOne) {
			for (final PartitionData pdTwo : setTwo) {
				if (pdOne.getId().equals(
						pdTwo.getId())) {
					return true;
				}
			}
		}
		return false;
	}

	private List<PartitionData> getPartitionDataFor(
			final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults,
			final String id,
			final boolean primary ) {
		final ArrayList<PartitionData> results = new ArrayList<PartitionData>();
		for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
			if (((FeatureWritable) pair.getSecond().getObjectWritable().get()).getFeature().getID().equals(
					id) && (pair.getFirst().partitionData.isPrimary() == primary)) {
				results.add(pair.getFirst().partitionData);
			}
		}
		return results;
	}

}
