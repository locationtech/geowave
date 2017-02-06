package mil.nga.giat.geowave.analytic.clustering;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class NestedGroupCentroidAssignmentTest
{

	private <T> void ingest(
			final DataStore dataStore,
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final T entry )
			throws IOException {
		try (IndexWriter writer = dataStore.createWriter(
				adapter,
				index)) {
			writer.write(entry);
			writer.close();
		}
	}

	@Test
	public void test()
			throws IOException {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();
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

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				ftype);
		final String namespace = "test_" + getClass().getName();
		final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
		StoreFactoryOptions opts = storeFamily.getDataStoreFactory().createOptionsInstance();
		opts.setGeowaveNamespace(namespace);
		final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(
				opts);
		final IndexStore indexStore = storeFamily.getIndexStoreFactory().createStore(
				opts);
		final AdapterStore adapterStore = storeFamily.getAdapterStoreFactory().createStore(
				opts);

		ingest(
				dataStore,
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
		ingest(
				dataStore,
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
		ingest(
				dataStore,
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
		ingest(
				dataStore,
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
		ingest(
				dataStore,
				adapter,
				index,
				level2B2G1Feature);

		final SimpleFeatureItemWrapperFactory wrapperFactory = new SimpleFeatureItemWrapperFactory();
		final CentroidManagerGeoWave<SimpleFeature> mananger = new CentroidManagerGeoWave<SimpleFeature>(
				dataStore,
				indexStore,
				adapterStore,
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
				dataStore,
				indexStore,
				adapterStore,
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
