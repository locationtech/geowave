package mil.nga.giat.geowave.store.adapter;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import mil.nga.giat.geowave.store.adapter.AbstractDataAdapterTest.MockAbstractDataAdapter;
import mil.nga.giat.geowave.store.adapter.AbstractDataAdapterTest.TestNativeFieldHandler;
import mil.nga.giat.geowave.store.adapter.AbstractDataAdapterTest.TestPersistentIndexFieldHandler;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

import org.junit.Test;

public class MemoryDataAdapterStoreTest
{
	@Test
	public void test()
			throws IOException,
			ClassNotFoundException {
		final ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>> indexFieldHandlers = new ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>>();
		indexFieldHandlers.add(new TestPersistentIndexFieldHandler());

		final ArrayList<NativeFieldHandler<Integer, Object>> nativeFieldHandlers = new ArrayList<NativeFieldHandler<Integer, Object>>();
		nativeFieldHandlers.add(new TestNativeFieldHandler());

		final MockAbstractDataAdapter mockAbstractDataAdapter = new MockAbstractDataAdapter(
				indexFieldHandlers,
				nativeFieldHandlers);
		MemoryAdapterStore store = new MemoryAdapterStore(
				new DataAdapter<?>[] {
					mockAbstractDataAdapter
				});

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
			MemoryAdapterStore storeReplica = (MemoryAdapterStore) is.readObject();
			assertNotNull(storeReplica.getAdapter(mockAbstractDataAdapter.getAdapterId()));
		}
	}

}
