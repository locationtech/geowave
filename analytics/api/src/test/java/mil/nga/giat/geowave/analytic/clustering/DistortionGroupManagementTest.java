package mil.nga.giat.geowave.analytic.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class DistortionGroupManagementTest
{

	final GeometryFactory factory = new GeometryFactory();
	final SimpleFeatureType ftype;
	final BasicAccumuloOperations dataOps;
	final MockInstance mockDataInstance = new MockInstance();
	final Connector mockDataConnector;
	final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

	final FeatureDataAdapter adapter;
	final AccumuloDataStore dataStore;

	public DistortionGroupManagementTest() {
		ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getType();

		try {
			mockDataConnector = mockDataInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (final Exception e) {
			throw new RuntimeException(
					e);
		}
		dataOps = new BasicAccumuloOperations(
				mockDataConnector);

		adapter = new FeatureDataAdapter(
				ftype);

		dataStore = new AccumuloDataStore(
				dataOps);
		dataOps.createTable("DIS");

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				dataOps);
		adapterStore.addAdapter(adapter);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				dataOps);
		indexStore.addIndex(index);
	}

	private void putMutation(
			final String grp,
			final long count,
			final Double distortion,
			final BatchWriter writer )
			throws MutationsRejectedException {
		final Mutation m = new Mutation(
				grp);
		m.put(
				new Text(
						"dt"),
				new Text(
						Long.toString(count)),
				new Value(
						distortion.toString().getBytes()));
		writer.addMutation(m);

	}

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {

		final BatchWriterConfig config = new BatchWriterConfig();
		final BatchWriter writer = mockDataConnector.createBatchWriter(
				"DIS",
				config);
		// big jump for grp1 between batch 2 and 3
		// big jump for grp2 between batch 1 and 2
		// thus, the jump occurs for different groups between different batches!

		// b1
		putMutation(
				"grp1",
				1,
				0.1,
				writer);
		putMutation(
				"grp2",
				1,
				0.1,
				writer);
		// b2
		putMutation(
				"grp1",
				2,
				0.2,
				writer);
		putMutation(
				"grp2",
				2,
				0.3,
				writer);
		// b3
		putMutation(
				"grp1",
				3,
				0.4,
				writer);
		putMutation(
				"grp2",
				3,
				0.4,
				writer);

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"123",
						"fred",
						"grp1",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"124",
						"barney",
						"grp1",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"125",
						"wilma",
						"grp2",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"126",
						"betty",
						"grp2",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"130",
						"dusty",
						"grp1",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"131",
						"dino",
						"grp1",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"127",
						"bamm-bamm",
						"grp2",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"128",
						"chip",
						"grp2",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"140",
						"pearl",
						"grp1",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"141",
						"roxy",
						"grp1",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"142",
						"giggles",
						"grp2",
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
						0));

		dataStore.ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"143",
						"gazoo",
						"grp2",
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
						0));

	}

	@Test
	public void test()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		DistortionGroupManagement.retainBestGroups(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"DIS",
				"b1",
				1);
		final CentroidManagerGeoWave<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);
		List<String> groups = centroidManager.getAllCentroidGroups();
		assertEquals(
				2,
				groups.size());
		boolean groupFound[] = new boolean[2];
		for (String grpId : groups) {
			List<AnalyticItemWrapper<SimpleFeature>> items = centroidManager.getCentroidsForGroup(grpId);
			assertEquals(
					2,
					items.size());
			if ("grp1".equals(grpId)) {
				groupFound[0] = true;
				assertTrue("pearl".equals(items.get(
						0).getName()) || "roxy".equals(items.get(
						0).getName()));
			}
			else if ("grp2".equals(grpId)) {
				groupFound[1] = true;
				assertTrue("chip".equals(items.get(
						0).getName()) || "bamm-bamm".equals(items.get(
						0).getName()));
			}
		}
		// each unique group is found?
		int c = 0;
		for (boolean gf : groupFound) {
			c += (gf ? 1 : 0);
		}
		assertEquals(
				2,
				c);
	}
}
