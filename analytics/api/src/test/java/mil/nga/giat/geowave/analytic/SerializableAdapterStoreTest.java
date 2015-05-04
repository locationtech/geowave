package mil.nga.giat.geowave.analytic;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.SerializableAdapterStore;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.util.Assert;

public class SerializableAdapterStoreTest
{
	@Test
	public void testSerialization()
			throws ClassNotFoundException,
			IOException {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getType();
		SerializableAdapterStore store = new SerializableAdapterStore(
				new MemoryAdapterStore(
						new DataAdapter<?>[] {
							new FeatureDataAdapter(
									ftype)
						}));

		final ByteArrayId id = new ByteArrayId(
				"centroid");
		assertNotNull(checkSerialization(
				store).getAdapter(
				id));

		store = new SerializableAdapterStore(
				new AccumuloAdapterStore(
						null));
		try {
			checkSerialization(
					store).getAdapter(
					id);
		}
		catch (final IllegalStateException ex) {
			return;
		}
		Assert.shouldNeverReachHere("Expected an illegal state exception");
	}

	private SerializableAdapterStore checkSerialization(
			final SerializableAdapterStore store )
			throws IOException,
			ClassNotFoundException {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (ObjectOutputStream os = new ObjectOutputStream(
				bos)) {
			os.writeObject(store);
			os.flush();
		}
		final ByteArrayInputStream bis = new ByteArrayInputStream(
				bos.toByteArray());
		try (ObjectInputStream is = new ObjectInputStream(
				bis)) {
			return (SerializableAdapterStore) is.readObject();
		}
	}
}
