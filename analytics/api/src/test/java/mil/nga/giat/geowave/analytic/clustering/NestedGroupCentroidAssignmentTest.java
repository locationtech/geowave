package mil.nga.giat.geowave.analytic.clustering;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
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

public class NestedGroupCentroidAssignmentTest
{

	@Test
	public void test()
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

		final SimpleFeature level1b1G1Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level1b1G1Feature",
				"fred",
				grp1,
				20.30203,
				factory.createPoint(new Coordinate(
						02.5,
						0.25)),
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
				level1b1G1Feature);

		final SimpleFeature level1b1G2Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level1b1G2Feature",
				"flood",
				grp2,
				20.30203,
				factory.createPoint(new Coordinate(
						02.03,
						0.2)),
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
				level1b1G2Feature);

		final SimpleFeature level2b1G1Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level2b1G1Feature",
				"flou",
				level1b1G1Feature.getID(),
				20.30203,
				factory.createPoint(new Coordinate(
						02.5,
						0.25)),
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
				level2b1G1Feature);

		final SimpleFeature level2b1G2Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level2b1G2Feature",
				"flapper",
				level1b1G2Feature.getID(),
				20.30203,
				factory.createPoint(new Coordinate(
						02.03,
						0.2)),
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
				level2b1G2Feature);

		// different batch
		final SimpleFeature level2B2G1Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"level2B2G1Feature",
				"flapper",
				level1b1G1Feature.getID(),
				20.30203,
				factory.createPoint(new Coordinate(
						02.63,
						0.25)),
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
				level2B2G1Feature);

		final SimpleFeatureItemWrapperFactory wrapperFactory = new SimpleFeatureItemWrapperFactory();
		final CentroidManagerGeoWave<SimpleFeature> mananger = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);

		final List<CentroidPairing<SimpleFeature>> capturedPairing = new ArrayList<CentroidPairing<SimpleFeature>>();
		final AssociationNotification<SimpleFeature> assoc = new AssociationNotification<SimpleFeature>() {
			@Override
			public void notify(
					final CentroidPairing<SimpleFeature> pairing ) {
				capturedPairing.add(pairing);
			}
		};

		final FeatureCentroidDistanceFn distanceFn = new FeatureCentroidDistanceFn();
		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB1 = new NestedGroupCentroidAssignment<SimpleFeature>(
				mananger,
				1,
				"b1",
				distanceFn);
		assigmentB1.findCentroidForLevel(
				wrapperFactory.create(level1b1G1Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level1b1G1Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB1L2G1 = new NestedGroupCentroidAssignment<SimpleFeature>(
				mananger,
				2,
				"b1",
				distanceFn);
		assigmentB1L2G1.findCentroidForLevel(
				wrapperFactory.create(level1b1G1Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level2b1G1Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

		// level 2 and different parent grouping
		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB1L2G2 = new NestedGroupCentroidAssignment<SimpleFeature>(
				mananger,
				2,
				"b1",
				distanceFn);
		assigmentB1L2G2.findCentroidForLevel(
				wrapperFactory.create(level1b1G2Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level2b1G2Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

		// level two with different batch than parent

		final CentroidManagerGeoWave<SimpleFeature> mananger2 = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b2",
				2);
		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB2L2 = new NestedGroupCentroidAssignment<SimpleFeature>(
				mananger2,
				2,
				"b1",
				distanceFn);

		assigmentB2L2.findCentroidForLevel(
				wrapperFactory.create(level1b1G1Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level2B2G1Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

	}
}
