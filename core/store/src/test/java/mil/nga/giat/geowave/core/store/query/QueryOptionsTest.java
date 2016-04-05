package mil.nga.giat.geowave.core.store.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

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
	public void testGetAdaptersWithMinimalSetOfIndices()
			throws IOException {
		final QueryOptions ops = new QueryOptions();
		final PrimaryIndex index1 = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel(
						"QOT_tm1"));
		final PrimaryIndex index2 = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel(
						"QOT_tm2"));
		final PrimaryIndex index3 = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel(
						"QOT_tm3"));

		final AdapterStore adapterStore = new AdapterStore() {

			@Override
			public void addAdapter(
					final DataAdapter<?> adapter ) {}

			@Override
			public DataAdapter<?> getAdapter(
					final ByteArrayId adapterId ) {
				final MockComponents.MockAbstractDataAdapter adapter = new MockComponents.MockAbstractDataAdapter() {
					@Override
					public ByteArrayId getAdapterId() {
						return adapterId;
					}
				};
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
						Arrays.asList(
								this.getAdapter(new ByteArrayId(
										"QOT_1")),
								this.getAdapter(new ByteArrayId(
										"QOT_2")),
								this.getAdapter(new ByteArrayId(
										"QOT_3"))).iterator());
			}

		};

		final List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> result = ops.getAdaptersWithMinimalSetOfIndices(
				adapterStore,
				new AdapterIndexMappingStore() {

					@Override
					public AdapterToIndexMapping getIndicesForAdapter(
							ByteArrayId adapterId ) {
						if (adapterId.getString().equals(
								"QOT_1"))
							return new AdapterToIndexMapping(
									adapterId,
									new PrimaryIndex[] {
										index1,
										index2
									});
						else if (adapterId.getString().equals(
								"QOT_2")) return new AdapterToIndexMapping(
								adapterId,
								new PrimaryIndex[] {
									index1,
									index3
								});
						return new AdapterToIndexMapping(
								adapterId,
								new PrimaryIndex[] {
									index2,
									index3
								});
					}

					@Override
					public void addAdapterIndexMapping(
							AdapterToIndexMapping mapping )
							throws MismatchedIndexToAdapterMapping {}

					@Override
					public void remove(
							ByteArrayId adapterId ) {

					}
				},
				new IndexStore() {

					@Override
					public void addIndex(
							Index<?, ?> index ) {}

					@Override
					public Index<?, ?> getIndex(
							ByteArrayId indexId ) {
						if (indexId.equals(index1.getId()))
							return index1;
						else if (indexId.equals(index2.getId()))
							return index2;
						else if (indexId.equals(index3.getId())) return index3;
						return null;
					}

					@Override
					public boolean indexExists(
							ByteArrayId indexId ) {
						if (indexId.equals(index1.getId()))
							return true;
						else if (indexId.equals(index2.getId()))
							return true;
						else if (indexId.equals(index3.getId())) return true;
						return false;
					}

					@Override
					public CloseableIterator<Index<?, ?>> getIndices() {
						return new CloseableIterator.Wrapper(
								Arrays.asList(
										index1,
										index2,
										index3).iterator());
					}

				});

		assertEquals(
				2,
				result.size());
		assertEquals(
				index1,
				result.get(
						0).getLeft());
		result.get(
				0).getRight().contains(
				adapterStore.getAdapter(new ByteArrayId(
						"QOT_1")));
		result.get(
				0).getRight().contains(
				adapterStore.getAdapter(new ByteArrayId(
						"QOT_2")));
		assertEquals(
				index2,
				result.get(
						1).getLeft());
		result.get(
				1).getRight().contains(
				adapterStore.getAdapter(new ByteArrayId(
						"QOT_3")));
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
							final DataAdapter<?> adapter ) {}

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
				}));
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

	@Test
	public void testFieldIdSerialization() {
		final List<String> fieldIds = Arrays.asList(new String[] {
			"one",
			"two",
			"three"
		});
		final QueryOptions ops = new QueryOptions();
		ops.setFieldIds(fieldIds);
		final QueryOptions deserialized = new QueryOptions();
		deserialized.fromBinary(ops.toBinary());
		Assert.assertTrue(fieldIds.size() == deserialized.getFieldIds().size());
		Assert.assertTrue(fieldIds.equals(deserialized.getFieldIds()));
	}
}
