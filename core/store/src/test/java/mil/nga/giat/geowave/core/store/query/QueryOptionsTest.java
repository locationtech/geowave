/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.base.BaseQueryOptions;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
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

		final InternalAdapterStore internalAdapterStore = new InternalAdapterStore() {

			BiMap<ByteArrayId, Short> cache = HashBiMap.create();

			@Override
			public ByteArrayId getAdapterId(
					final short internalAdapterId ) {
				// TODO Auto-generated method stub
				return cache.inverse().get(
						internalAdapterId);
			}

			@Override
			public Short getInternalAdapterId(
					final ByteArrayId adapterId ) {
				// TODO Auto-generated method stub
				return cache.get(adapterId);
			}

			@Override
			public short addAdapterId(
					final ByteArrayId adapterId ) {
				// TODO Auto-generated method stub
				final Short id = cache.get(adapterId);
				if (id != null) {
					return id;
				}

				cache.put(
						new ByteArrayId(
								"QOT_1"),
						(short) 1);
				cache.put(
						new ByteArrayId(
								"QOT_2"),
						(short) 2);
				cache.put(
						new ByteArrayId(
								"QOT_3"),
						(short) 3);

				return cache.get(adapterId);

			}

			@Override
			public boolean remove(
					final ByteArrayId adapterId ) {
				return false;
			}

			@Override
			public boolean remove(
					final short internalAdapterId ) {
				return false;
			}

			@Override
			public void removeAll() {

			}
		};

		final PersistentAdapterStore adapterStore = new PersistentAdapterStore() {

			@Override
			public void addAdapter(
					final InternalDataAdapter<?> adapter ) {}

			@Override
			public InternalDataAdapter<?> getAdapter(
					final Short internalAdapterId ) {

				final ByteArrayId adapterId = internalAdapterStore.getAdapterId(internalAdapterId);
				final MockComponents.MockAbstractDataAdapter adapter = new MockComponents.MockAbstractDataAdapter() {
					@Override
					public ByteArrayId getAdapterId() {
						return adapterId;
					}
				};

				final InternalDataAdapter<?> internalDataAdapter = new InternalDataAdapterWrapper<>(
						adapter,
						internalAdapterId);
				return internalDataAdapter;

			}

			@Override
			public boolean adapterExists(
					final Short internalAdapterId ) {
				return true;
			}

			@Override
			public CloseableIterator<InternalDataAdapter<?>> getAdapters() {
				return new CloseableIterator.Wrapper(
						Arrays.asList(
								getAdapter(new Short(
										(short) 1)),
								getAdapter(new Short(
										(short) 2)),
								getAdapter(new Short(
										(short) 3))).iterator());
			}

			@Override
			public void removeAll() {}

			@Override
			public void removeAdapter(
					final Short adapterId ) {}

		};

		final BaseQueryOptions bops = new BaseQueryOptions(
				ops,
				internalAdapterStore);
		final List<Pair<PrimaryIndex, List<InternalDataAdapter<?>>>> result = bops.getAdaptersWithMinimalSetOfIndices(
				adapterStore,
				new AdapterIndexMappingStore() {

					@Override
					public AdapterToIndexMapping getIndicesForAdapter(
							final short adapterId ) {
						if (adapterId == 1) {
							return new AdapterToIndexMapping(
									adapterId,
									new PrimaryIndex[] {
										index1,
										index2
									});
						}
						else if (adapterId == 2) {
							return new AdapterToIndexMapping(
									adapterId,
									new PrimaryIndex[] {
										index1,
										index3
									});
						}
						return new AdapterToIndexMapping(
								adapterId,
								new PrimaryIndex[] {
									index2,
									index3
								});
					}

					@Override
					public void addAdapterIndexMapping(
							final AdapterToIndexMapping mapping )
							throws MismatchedIndexToAdapterMapping {}

					@Override
					public void remove(
							final short adapterId ) {}

					@Override
					public void removeAll() {}
				},
				new IndexStore() {

					@Override
					public void addIndex(
							final Index<?, ?> index ) {}

					@Override
					public Index<?, ?> getIndex(
							final ByteArrayId indexId ) {
						if (indexId.equals(index1.getId())) {
							return index1;
						}
						else if (indexId.equals(index2.getId())) {
							return index2;
						}
						else if (indexId.equals(index3.getId())) {
							return index3;
						}
						return null;
					}

					@Override
					public boolean indexExists(
							final ByteArrayId indexId ) {
						if (indexId.equals(index1.getId())) {
							return true;
						}
						else if (indexId.equals(index2.getId())) {
							return true;
						}
						else if (indexId.equals(index3.getId())) {
							return true;
						}
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

					@Override
					public void removeAll() {}

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
				adapterStore.getAdapter(new Short(
						(short) 1)));
		result.get(
				0).getRight().contains(
				adapterStore.getAdapter(new Short(
						(short) 12)));
		assertEquals(
				index2,
				result.get(
						1).getLeft());
		result.get(
				1).getRight().contains(
				adapterStore.getAdapter(new Short(
						(short) 3)));
	}

	@Test
	public void testAdapter() {
		final QueryOptions ops = new QueryOptions();
		ops.setAdapter(new MockComponents.MockAbstractDataAdapter());
		final QueryOptions ops2 = new QueryOptions();
		ops2.fromBinary(ops.toBinary());
		assertTrue(ops2.getAdapterIds().get(
				0) != null);
	}

	@Test
	public void testAdapters()
			throws IOException {
		final AdapterStore adapterStore = new TransientAdapterStore() {

			@Override
			public void addAdapter(
					final DataAdapter<?> adapter ) {}

			@Override
			public DataAdapter<?> getAdapter(
					final ByteArrayId adapterId ) {
				return new MockComponents.MockAbstractDataAdapter(
						adapterId);
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

			@Override
			public void removeAll() {}

			@Override
			public void removeAdapter(
					final ByteArrayId adapterId ) {}
		};

		final QueryOptions ops = new QueryOptions(
				Arrays.asList(
						adapterStore.getAdapter(new ByteArrayId(
								"123")),
						adapterStore.getAdapter(new ByteArrayId(
								"567"))));
		assertEquals(
				2,
				ops.getAdapterIds().size());
		final QueryOptions ops2 = new QueryOptions();
		ops2.fromBinary(ops.toBinary());
		assertEquals(
				2,
				ops2.getAdapterIds().size());

	}

	@Test
	public void testFieldIdSerialization() {
		final List<String> fieldIds = Arrays.asList(new String[] {
			"one",
			"two",
			"three"
		});
		final ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>> indexFieldHandlers = new ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>>();
		indexFieldHandlers.add(new MockComponents.TestPersistentIndexFieldHandler());
		final ArrayList<NativeFieldHandler<Integer, Object>> nativeFieldHandlers = new ArrayList<NativeFieldHandler<Integer, Object>>();
		nativeFieldHandlers.add(new MockComponents.TestNativeFieldHandler());
		final MockComponents.MockAbstractDataAdapter mockAbstractDataAdapter = new MockComponents.MockAbstractDataAdapter(
				indexFieldHandlers,
				nativeFieldHandlers);
		final QueryOptions ops = new QueryOptions(
				fieldIds,
				mockAbstractDataAdapter);
		final QueryOptions deserialized = new QueryOptions();
		deserialized.fromBinary(ops.toBinary());
		Assert.assertTrue(fieldIds.size() == deserialized.getFieldIdsAdapterPair().getLeft().size());
		Assert.assertTrue(fieldIds.equals(deserialized.getFieldIdsAdapterPair().getLeft()));
		Assert.assertTrue(deserialized.getFieldIdsAdapterPair().getRight() instanceof AbstractDataAdapter<?>);
	}
}
