package mil.nga.giat.geowave.core.store.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;

public class QueryOptionsTest
{

	@Test
	public void testAuthorizations() {
		final QueryOptions ops = new QueryOptions();
		ops.setAuthorizations(new String[] {
			"12",
			"34"
		});
		ops.fromBinary(ops.toBinary());
		assertTrue(Arrays.asList(
				ops.getAuthorizations()).contains(
				"12"));
		assertTrue(Arrays.asList(
				ops.getAuthorizations()).contains(
				"34"));
	}

	@Test
	public void testAdapter() {
		final QueryOptions ops = new QueryOptions();
		ops.setAdapter(new MockComponents.MockAbstractDataAdapter());
		final QueryOptions ops2 = new QueryOptions();
		ops2.fromBinary(ops.toBinary());
		assertTrue(ops2.getAdapters(
				new AdapterStore() {

					@Override
					public void addAdapter(
							final DataAdapter<?> adapter ) {

					}

					@Override
					public DataAdapter<?> getAdapter(
							final ByteArrayId adapterId ) {
						final MockComponents.MockAbstractDataAdapter adapter = new MockComponents.MockAbstractDataAdapter();
						return adapter.getAdapterId().equals(
								adapterId) ? adapter : null;
					}

					@Override
					public boolean adapterExists(
							final ByteArrayId adapterId ) {
						return true;
					}

					@Override
					public CloseableIterator<DataAdapter<?>> getAdapters() {
						return new CloseableIterator.Wrapper(
								Collections.emptyListIterator());
					}
				}).next().getAdapterId() != null);
	}

	@Test
	public void testAdapterIds()
			throws IOException {
		final AdapterStore adapterStore = new AdapterStore() {

			@Override
			public void addAdapter(
					final DataAdapter<?> adapter ) {

			}

			@Override
			public DataAdapter<?> getAdapter(
					final ByteArrayId adapterId ) {
				final MockComponents.MockAbstractDataAdapter adapter = new MockComponents.MockAbstractDataAdapter();
				return adapter.getAdapterId().equals(
						adapterId) ? adapter : null;
			}

			@Override
			public boolean adapterExists(
					final ByteArrayId adapterId ) {
				return true;
			}

			@Override
			public CloseableIterator<DataAdapter<?>> getAdapters() {
				return new CloseableIterator.Wrapper(
						Collections.emptyListIterator());
			}
		};

		final QueryOptions ops = new QueryOptions(
				Arrays.asList(new ByteArrayId[] {
					new ByteArrayId(
							"123"),
					new ByteArrayId(
							"567")
				}),
				null);
		assertEquals(
				2,
				ops.getAdapterIds(
						adapterStore).size());
		final QueryOptions ops2 = new QueryOptions();
		ops2.fromBinary(ops.toBinary());
		assertEquals(
				2,
				ops2.getAdapterIds(
						adapterStore).size());

	}
}
