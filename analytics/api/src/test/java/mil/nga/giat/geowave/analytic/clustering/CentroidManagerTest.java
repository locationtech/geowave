package mil.nga.giat.geowave.analytic.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class CentroidManagerTest
{
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

	@Test
	public void testSampleRecall()
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
		ingest(
				dataStore,
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
		ingest(
				dataStore,
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
		ingest(
				dataStore,
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
		ingest(
				dataStore,
				adapter,
				index,
				feature);

		CentroidManagerGeoWave<SimpleFeature> manager = new CentroidManagerGeoWave<SimpleFeature>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);
		List<AnalyticItemWrapper<SimpleFeature>> centroids = manager.getCentroidsForGroup(null);

		assertEquals(
				3,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		centroids = manager.getCentroidsForGroup(grp1);
		assertEquals(
				2,
				centroids.size());
		centroids = manager.getCentroidsForGroup(grp2);
		assertEquals(
				1,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		manager = new CentroidManagerGeoWave<SimpleFeature>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);

		manager.processForAllGroups(new CentroidProcessingFn<SimpleFeature>() {

			@Override
			public int processGroup(
					final String groupID,
					final List<AnalyticItemWrapper<SimpleFeature>> centroids ) {
				if (groupID.equals(grp1)) {
					assertEquals(
							2,
							centroids.size());
				}
				else if (groupID.equals(grp2)) {
					assertEquals(
							1,
							centroids.size());
				}
				else {
					assertTrue(
							"what group is this : " + groupID,
							false);
				}
				return 0;
			}

		});

	}
}
