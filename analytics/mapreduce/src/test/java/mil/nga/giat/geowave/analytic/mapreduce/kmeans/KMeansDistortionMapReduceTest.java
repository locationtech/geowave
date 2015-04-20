package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureWritable;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytic.mapreduce.kmeans.KMeansDistortionMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
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

public class KMeansDistortionMapReduceTest
{
	MapDriver<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable> mapDriver;
	ReduceDriver<Text, CountofDoubleWritable, Text, Mutation> reduceDriver;

	final String batchId = "b1";

	final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
			"centroid",
			new String[] {
				"extra1"
			},
			"http://geowave.test.net",
			ClusteringUtils.CLUSTERING_CRS).getType();
	final FeatureDataAdapter testObjectAapter = new FeatureDataAdapter(
			ftype);

	private static final List<Object> capturedObjects = new ArrayList<Object>();

	final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

	final GeometryFactory factory = new GeometryFactory();
	final String grp1 = "g1";
	Key accumuloKey = null; // new Key(new Text("row"), new Text("cf"), new

	// Text("c"));

	@Before
	public void setUp()
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {
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
				testObjectAapter);

		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				testObjectAapter);

		PropertyManagement propManagement = new PropertyManagement();
		propManagement.store(
				CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
				MockAccumuloOperationsFactory.class);
		propManagement.store(
				CentroidParameters.Centroid.INDEX_ID,
				IndexType.SPATIAL_VECTOR.getDefaultId());
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

		NestedGroupCentroidAssignment.setParameters(
				reduceDriver.getConfiguration(),
				propManagement);
		NestedGroupCentroidAssignment.setParameters(
				mapDriver.getConfiguration(),
				propManagement);

		serializations();

		final BasicAccumuloOperations dataOps = new MockAccumuloOperationsFactory().build(
				null,
				null,
				null,
				null,
				null);

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

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				dataOps);
		dataStore.ingest(
				testObjectAapter,
				index,
				feature);

		CentroidManagerGeoWave.setParameters(
				reduceDriver.getConfiguration(),
				dataOps.getConnector().getInstance().getZooKeepers(),
				dataOps.getConnector().getInstance().getInstanceName(),
				"root",
				"",
				"",
				SimpleFeatureItemWrapperFactory.class,
				ftype.getTypeName(),
				IndexType.SPATIAL_VECTOR.getDefaultId(),
				batchId,
				1);
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
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {

		final GeoWaveInputKey inputKey = new GeoWaveInputKey();
		inputKey.setAccumuloKey(accumuloKey);
		inputKey.setAdapterId(testObjectAapter.getAdapterId());
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
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {

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

		final List<Pair<Text, Mutation>> results = reduceDriver.run();
		assertEquals(
				1,
				results.size());

		final Mutation m2 = new Mutation(
				"g1");
		// TODO: floating point error?
		m2.put(
				new Text(
						"dt"),
				new Text(
						"1"),
				new Value(
						"3.6697247706422016".toString().getBytes()));
		assertTrue(results.get(
				0).getSecond().equals(
				m2));
	}
}
