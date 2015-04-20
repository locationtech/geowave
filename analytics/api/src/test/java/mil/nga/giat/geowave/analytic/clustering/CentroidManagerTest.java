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
import mil.nga.giat.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class CentroidManagerTest
{
	@Test
	public void testSampleRecall()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		final MockInstance mockDataInstance = new MockInstance();
		final Connector mockDataConnector = mockDataInstance.getConnector(
				"root",
				new PasswordToken(
						new byte[0]));

		final BasicAccumuloOperations dataOps = new BasicAccumuloOperations(
				mockDataConnector);

		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getType();
		final GeometryFactory factory = new GeometryFactory();
		final String grp1 = "g1";
		final String grp2 = "g2";
		SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
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

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				ftype);

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				dataOps);
		dataStore.ingest(
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"231",
				"flood",
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
		dataStore.ingest(
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"321",
				"flou",
				grp2,
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
		dataStore.ingest(
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"312",
				"flapper",
				grp2,
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
		dataStore.ingest(
				adapter,
				index,
				feature);

		// and one feature with a different zoom level
		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"312",
				"flapper",
				grp2,
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
				2,
				1,
				0);
		dataStore.ingest(
				adapter,
				index,
				feature);

		CentroidManagerGeoWave<SimpleFeature> mananger = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);
		List<AnalyticItemWrapper<SimpleFeature>> centroids = mananger.getCentroidsForGroup(null);

		assertEquals(
				3,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		centroids = mananger.getCentroidsForGroup(grp1);
		assertEquals(
				2,
				centroids.size());
		centroids = mananger.getCentroidsForGroup(grp2);
		assertEquals(
				1,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		mananger = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);

		mananger.processForAllGroups(new CentroidProcessingFn<SimpleFeature>() {

			@Override
			public int processGroup(
					String groupID,
					List<AnalyticItemWrapper<SimpleFeature>> centroids ) {
				if (groupID.equals(grp1))
					assertEquals(
							2,
							centroids.size());
				else if (groupID.equals(grp2))
					assertEquals(
							1,
							centroids.size());
				else
					assertTrue(
							"what group is this : " + groupID,
							false);
				return 0;
			}

		});

	}
}
