package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureWritable;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.DistortionEntry;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class KMeansDistortionMapReduceTest
{
	private static final String TEST_NAMESPACE = "test";
	MapDriver<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable> mapDriver;
	ReduceDriver<Text, CountofDoubleWritable, GeoWaveOutputKey, DistortionEntry> reduceDriver;

	final String batchId = "b1";

	final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
			"centroid",
			new String[] {
				"extra1"
			},
			"http://geowave.test.net",
			ClusteringUtils.CLUSTERING_CRS).getFeatureType();
	final FeatureDataAdapter testObjectAdapter = new FeatureDataAdapter(
			ftype);

	private static final List<Object> capturedObjects = new ArrayList<Object>();

	final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

	final GeometryFactory factory = new GeometryFactory();
	final String grp1 = "g1";

	@Before
	public void setUp()
			throws IOException {
		final KMeansDistortionMapReduce.KMeansDistortionMapper mapper = new KMeansDistortionMapReduce.KMeansDistortionMapper();
		final KMeansDistortionMapReduce.KMeansDistortionReduce reducer = new KMeansDistortionMapReduce.KMeansDistortionReduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);

		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KMeansDistortionMapReduce.class,
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS),
				FeatureCentroidDistanceFn.class,
				DistanceFn.class);

		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				testObjectAdapter);

		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				testObjectAdapter);

		final PropertyManagement propManagement = new PropertyManagement();
		propManagement.store(
				CentroidParameters.Centroid.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex().getId().getString());
		propManagement.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				ftype.getTypeName());

		propManagement.store(
				CentroidParameters.Centroid.DATA_NAMESPACE_URI,
				ftype.getName().getNamespaceURI());
		propManagement.store(
				GlobalParameters.Global.BATCH_ID,
				batchId);
		propManagement.store(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);
		propManagement.store(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);

		DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		pluginOptions.selectPlugin("memory");
		MemoryRequiredOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
		opts.setGeowaveNamespace(TEST_NAMESPACE);
		PersistableStore store = new PersistableStore(
				pluginOptions);

		propManagement.store(
				StoreParam.INPUT_STORE,
				store);

		NestedGroupCentroidAssignment.setParameters(
				mapDriver.getConfiguration(),
				KMeansDistortionMapReduce.class,
				propManagement);

		serializations();

		capturedObjects.clear();

		final SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				batchId,
				"123",
				"fred",
				grp1,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

		propManagement.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				1);
		ingest(
				pluginOptions.createDataStore(),
				testObjectAdapter,
				index,
				feature);

		CentroidManagerGeoWave.setParameters(
				reduceDriver.getConfiguration(),
				KMeansDistortionMapReduce.class,
				propManagement);
	}

	private void ingest(
			final DataStore dataStore,
			final FeatureDataAdapter adapter,
			final PrimaryIndex index,
			final SimpleFeature feature )
			throws IOException {
		try (IndexWriter writer = dataStore.createWriter(
				adapter,
				index)) {
			writer.write(feature);
			writer.close();
		}
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

		final GeoWaveInputKey inputKey = new GeoWaveInputKey();
		inputKey.setInsertionId(null);
		inputKey.setAdapterId(testObjectAdapter.getAdapterId());
		inputKey.setDataId(new ByteArrayId(
				"abc".getBytes()));

		final ObjectWritable ow = new ObjectWritable();
		ow.set(new FeatureWritable(
				ftype,
				AnalyticFeature.createGeometryFeature(
						ftype,
						batchId,
						"123",
						"fred",
						grp1,
						20.30203,
						factory.createPoint(new Coordinate(
								02.33,
								0.23)),
						new String[] {
							"extra1"
						},
						new double[] {
							0.022
						},
						1,
						1,
						0)));

		mapDriver.withInput(
				inputKey,
				ow);

		final List<Pair<Text, CountofDoubleWritable>> results = mapDriver.run();
		// output key has the dataID adjusted to contain the rank
		assertEquals(
				results.get(
						0).getFirst().toString(),
				grp1);
		// output value is the same as input value
		assertEquals(
				results.get(
						0).getSecond().getValue(),
				0.0,
				0.0001);

	}

	@Test
	public void testReducer()
			throws IOException {

		reduceDriver.addInput(
				new Text(
						"g1"),
				Arrays.asList(
						new CountofDoubleWritable(
								0.34,
								1),
						new CountofDoubleWritable(
								0.75,
								1)));
		reduceDriver.addInput(
				new Text(
						"g2"),
				Arrays.asList(
						new CountofDoubleWritable(
								0.34,
								1),
						new CountofDoubleWritable(
								0.25,
								1)));

		final List<Pair<GeoWaveOutputKey, DistortionEntry>> results = reduceDriver.run();
		assertEquals(
				1,
				results.size());

		assertTrue(results.get(
				0).getSecond().getGroupId().equals(
				"g1"));
		assertTrue(results.get(
				0).getSecond().getClusterCount().equals(
				1));
		// TODO: floating point error?
		assertTrue(results.get(
				0).getSecond().getDistortionValue().equals(
				3.6697247706422016));
	}
}
