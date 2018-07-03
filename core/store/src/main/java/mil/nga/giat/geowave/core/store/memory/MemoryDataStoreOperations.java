package mil.nga.giat.geowave.core.store.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.data.DeferredReadCommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.UnreadFieldDataList;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class MemoryDataStoreOperations implements
		DataStoreOperations
{
	private final static Logger LOGGER = Logger.getLogger(MemoryDataStoreOperations.class);
	private final Map<ByteArrayId, SortedSet<MemoryStoreEntry>> storeData = Collections
			.synchronizedMap(new HashMap<ByteArrayId, SortedSet<MemoryStoreEntry>>());
	private final Map<MetadataType, SortedSet<MemoryMetadataEntry>> metadataStore = Collections
			.synchronizedMap(new HashMap<MetadataType, SortedSet<MemoryMetadataEntry>>());
	private final boolean serversideEnabled;

	public MemoryDataStoreOperations() {
		this(
				true);
	}

	public MemoryDataStoreOperations(
			final boolean serversideEnabled ) {
		this.serversideEnabled = serversideEnabled;
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		if (AbstractGeoWavePersistence.METADATA_TABLE.equals(indexId.getString())) {
			return !metadataStore.isEmpty();
		}
		return storeData.containsKey(indexId);
	}

	@Override
	public void deleteAll()
			throws Exception {
		storeData.clear();
	}

	@Override
	public boolean deleteAll(
			final ByteArrayId tableName,
			final Short internalAdapterId,
			final String... additionalAuthorizations ) {
		return false;
	}

	@Override
	public boolean ensureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public Writer createWriter(
			final ByteArrayId indexId,
			final short adapterId ) {
		return new MyIndexWriter<>(
				indexId);
	}

	@Override
	public Deleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		return new MyIndexDeleter(
				indexId,
				authorizations);
	}

	protected SortedSet<MemoryStoreEntry> getRowsForIndex(
			final ByteArrayId id ) {
		SortedSet<MemoryStoreEntry> set = storeData.get(id);
		if (set == null) {
			set = Collections.synchronizedSortedSet(new TreeSet<MemoryStoreEntry>());
			storeData.put(
					id,
					set);
		}
		return set;
	}

	@Override
	public <T> Reader<T> createReader(
			final ReaderParams<T> readerParams ) {
		final SortedSet<MemoryStoreEntry> internalData = storeData.get(readerParams.getIndex().getId());
		int counter = 0;
		List<MemoryStoreEntry> retVal = new ArrayList<>();
		final Collection<SinglePartitionQueryRanges> partitionRanges = readerParams
				.getQueryRanges()
				.getPartitionQueryRanges();
		if ((partitionRanges == null) || partitionRanges.isEmpty()) {
			retVal.addAll(internalData);
			// remove unauthorized
			final Iterator<MemoryStoreEntry> it = retVal.iterator();
			while (it.hasNext()) {
				if (!isAuthorized(
						it.next(),
						readerParams.getAdditionalAuthorizations())) {
					it.remove();
				}
			}
			if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)
					&& (retVal.size() > readerParams.getLimit())) {
				retVal = retVal.subList(
						0,
						readerParams.getLimit());
			}
		}
		else {
			for (final SinglePartitionQueryRanges p : partitionRanges) {
				for (final ByteArrayRange r : p.getSortKeyRanges()) {
					final SortedSet<MemoryStoreEntry> set;
					if (r.isSingleValue()) {
						set = internalData.subSet(
								new MemoryStoreEntry(
										p.getPartitionKey(),
										r.getStart()),
								new MemoryStoreEntry(
										p.getPartitionKey(),
										new ByteArrayId(
												r.getStart().getNextPrefix())));
					}
					else {
						set = internalData.tailSet(
								new MemoryStoreEntry(
										p.getPartitionKey(),
										r.getStart())).headSet(
								new MemoryStoreEntry(
										p.getPartitionKey(),
										r.getEndAsNextPrefix()));
					}
					// remove unauthorized
					final Iterator<MemoryStoreEntry> it = set.iterator();
					while (it.hasNext()) {
						if (!isAuthorized(
								it.next(),
								readerParams.getAdditionalAuthorizations())) {
							it.remove();
						}
					}
					if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)
							&& ((counter + set.size()) > readerParams.getLimit())) {
						final List<MemoryStoreEntry> subset = new ArrayList<>(
								set);
						retVal.addAll(subset.subList(
								0,
								readerParams.getLimit() - counter));
						break;
					}
					else {
						retVal.addAll(set);
						counter += set.size();
						if ((readerParams.getLimit() > 0) && (counter >= readerParams.getLimit())) {
							break;
						}
					}
				}
			}
		}
		return new MyIndexReader(
				Iterators.filter(
						retVal.iterator(),
						new Predicate<MemoryStoreEntry>() {
							@Override
							public boolean apply(
									final MemoryStoreEntry input ) {
								if ((readerParams.getFilter() != null) && serversideEnabled) {
									final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<>();
									final List<FlattenedUnreadData> unreadData = new ArrayList<>();
									final List<ByteArrayId> commonIndexFieldIds = DataStoreUtils
											.getUniqueDimensionFields(readerParams.getIndex().getIndexModel());
									for (final GeoWaveValue v : input.getRow().getFieldValues()) {
										unreadData.add(DataStoreUtils.aggregateFieldData(
												input.getRow(),
												v,
												commonData,
												readerParams.getIndex().getIndexModel(),
												commonIndexFieldIds));
									}
									return readerParams.getFilter().accept(
											readerParams.getIndex().getIndexModel(),
											new DeferredReadCommonIndexedPersistenceEncoding(
													input.getRow().getInternalAdapterId(),
													new ByteArrayId(
															input.getRow().getDataId()),
													new ByteArrayId(
															input.getRow().getPartitionKey()),
													new ByteArrayId(
															input.getRow().getSortKey()),
													input.getRow().getNumberOfDuplicates(),
													commonData,
													unreadData.isEmpty() ? null : new UnreadFieldDataList(
															unreadData)));
								}
								return true;
							}
						}),
				readerParams.getRowTransformer());
	}

	private boolean isAuthorized(
			final MemoryStoreEntry row,
			final String... authorizations ) {
		for (final GeoWaveValue value : row.getRow().getFieldValues()) {
			if (!MemoryStoreUtils.isAuthorized(
					value.getVisibility(),
					authorizations)) {
				return false;
			}
		}
		return true;
	}

	private static class MyIndexReader<T> implements
			Reader<T>
	{
		private final Iterator<T> it;

		public MyIndexReader(
				final Iterator<MemoryStoreEntry> it,
				final GeoWaveRowIteratorTransformer<T> rowTransformer ) {
			super();
			this.it = rowTransformer.apply(Iterators.transform(it, e -> e.row));
		}

		@Override
		public void close()
				throws Exception {}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			return it.next();
		}
	}

	private class MyIndexWriter<T> implements
			Writer
	{
		final ByteArrayId indexId;

		public MyIndexWriter(
				final ByteArrayId indexId ) {
			super();
			this.indexId = indexId;
		}

		@Override
		public void close()
				throws IOException {}

		@Override
		public void flush() {
			try {
				close();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Error closing index writer",
						e);
			}
		}

		@Override
		public void write(
				final GeoWaveRow[] rows ) {
			for (final GeoWaveRow r : rows) {
				write(r);
			}
		}

		@Override
		public void write(
				final GeoWaveRow row ) {
			SortedSet<MemoryStoreEntry> rowTreeSet = storeData.get(indexId);
			if (rowTreeSet == null) {
				rowTreeSet = new TreeSet<>();
				storeData.put(
						indexId,
						rowTreeSet);
			}
			if (rowTreeSet.contains(new MemoryStoreEntry(
					row))) {
				rowTreeSet.remove(new MemoryStoreEntry(
						row));
			}
			if (!rowTreeSet.add(new MemoryStoreEntry(
					row))) {
				LOGGER.warn("Unable to add new entry");
			}
		}
	}

	private class MyIndexDeleter implements
			Deleter
	{
		private final ByteArrayId indexId;
		private final String[] authorizations;

		public MyIndexDeleter(
				final ByteArrayId indexId,
				final String... authorizations ) {
			this.indexId = indexId;
			this.authorizations = authorizations;
		}

		@Override
		public void close()
				throws Exception {}

		@Override
		public void delete(
				final GeoWaveRow row,
				final DataAdapter<?> adapter ) {
			final MemoryStoreEntry entry = new MemoryStoreEntry(
					row);
			if (isAuthorized(
					entry,
					authorizations)) {
				final SortedSet<MemoryStoreEntry> rowTreeSet = storeData.get(indexId);
				if (rowTreeSet != null) {
					if (!rowTreeSet.remove(entry)) {
						LOGGER.warn("Unable to remove entry");
					}
				}
			}
		}
	}

	public static class MemoryStoreEntry implements
			Comparable<MemoryStoreEntry>
	{
		private final GeoWaveRow row;

		public MemoryStoreEntry(
				final ByteArrayId comparisonPartitionKey,
				final ByteArrayId comparisonSortKey ) {
			row = new GeoWaveRowImpl(
					new GeoWaveKeyImpl(
							new byte[] {
								0
							},
							(short) 0, // new byte[] {},
							comparisonPartitionKey.getBytes(),
							comparisonSortKey.getBytes(),
							0),
					null);
		}

		public MemoryStoreEntry(
				final GeoWaveRow row ) {
			this.row = row;
		}

		public GeoWaveRow getRow() {
			return row;
		}

		public byte[] getCompositeInsertionId() {
			return ((GeoWaveKeyImpl) ((GeoWaveRowImpl) row).getKey()).getCompositeInsertionId();
		}

		@Override
		public int compareTo(
				final MemoryStoreEntry other ) {
			final int indexIdCompare = UnsignedBytes.lexicographicalComparator().compare(
					getCompositeInsertionId(),
					other.getCompositeInsertionId());
			if (indexIdCompare != 0) {
				return indexIdCompare;
			}
			final int dataIdCompare = UnsignedBytes.lexicographicalComparator().compare(
					row.getDataId(),
					other.getRow().getDataId());
			if (dataIdCompare != 0) {
				return dataIdCompare;
			}
			final int adapterIdCompare = UnsignedBytes.lexicographicalComparator().compare(
					ByteArrayUtils.shortToByteArray(row.getInternalAdapterId()),
					ByteArrayUtils.shortToByteArray(other.getRow().getInternalAdapterId()));
			if (adapterIdCompare != 0) {
				return adapterIdCompare;
			}
			return 0;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((row == null) ? 0 : row.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final MemoryStoreEntry other = (MemoryStoreEntry) obj;
			if (row == null) {
				if (other.row != null) {
					return false;
				}
			}
			return compareTo(other) == 0;
		}
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		return new MyMetadataWriter<>(
				metadataType);
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new MyMetadataReader(
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new MyMetadataDeleter(
				metadataType);
	}

	private class MyMetadataReader implements
			MetadataReader
	{
		private final MetadataType type;

		public MyMetadataReader(
				final MetadataType type ) {
			super();
			this.type = type;
		}

		@SuppressWarnings({
			"rawtypes",
			"unchecked"
		})
		@Override
		public CloseableIterator<GeoWaveMetadata> query(
				final MetadataQuery query ) {
			final SortedSet<MemoryMetadataEntry> typeStore = metadataStore.get(type);
			if (typeStore == null) {
				return new CloseableIterator.Empty<GeoWaveMetadata>();
			}
			final SortedSet<MemoryMetadataEntry> set = typeStore.subSet(
					new MemoryMetadataEntry(
							new GeoWaveMetadata(
									query.getPrimaryId(),
									query.getSecondaryId(),
									null,
									null),
							null),
					new MemoryMetadataEntry(
							new GeoWaveMetadata(
									getNextPrefix(query.getPrimaryId()),
									getNextPrefix(query.getSecondaryId()),
									// this should be sufficient
									new byte[] {
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF
									},
									// this should be sufficient
									new byte[] {
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF
									}),
							new byte[] {
								(byte) 0xFF,
								(byte) 0xFF,
								(byte) 0xFF,
								(byte) 0xFF,
								(byte) 0xFF,
								(byte) 0xFF,
								(byte) 0xFF
							}));
			Iterator<MemoryMetadataEntry> it = set.iterator();
			if ((query.getAuthorizations() != null) && (query.getAuthorizations().length > 0)) {
				it = Iterators.filter(
						it,
						new Predicate<MemoryMetadataEntry>() {
							@Override
							public boolean apply(
									final MemoryMetadataEntry input ) {
								return MemoryStoreUtils.isAuthorized(
										input.getMetadata().getVisibility(),
										query.getAuthorizations());
							}
						});
			}
			final Iterator<GeoWaveMetadata> itTransformed = Iterators.transform(
					it,
					new Function<MemoryMetadataEntry, GeoWaveMetadata>() {
						@Override
						public GeoWaveMetadata apply(
								final MemoryMetadataEntry input ) {
							return input.metadata;
						}
					});
			if (MetadataType.STATS.equals(type)) {
				return new CloseableIterator.Wrapper(
						new Iterator<GeoWaveMetadata>() {
							final PeekingIterator<GeoWaveMetadata> peekingIt = Iterators.peekingIterator(itTransformed);

							@Override
							public boolean hasNext() {
								return peekingIt.hasNext();
							}

							@Override
							public GeoWaveMetadata next() {
								DataStatistics currentStat = null;
								GeoWaveMetadata currentMetadata = null;
								byte[] vis = null;
								while (peekingIt.hasNext()) {
									currentMetadata = peekingIt.next();
									vis = currentMetadata.getVisibility();
									if (!peekingIt.hasNext()) {
										break;
									}
									final GeoWaveMetadata next = peekingIt.peek();
									if (Objects.deepEquals(
											currentMetadata.getPrimaryId(),
											next.getPrimaryId()) && Objects.deepEquals(
											currentMetadata.getSecondaryId(),
											next.getSecondaryId())) {
										if (currentStat == null) {
											currentStat = (DataStatistics) PersistenceUtils.fromBinary(currentMetadata
													.getValue());
										}
										currentStat.merge((Mergeable) PersistenceUtils.fromBinary(next.getValue()));
										vis = combineVisibilities(
												vis,
												next.getVisibility());
									}
									else {
										break;
									}
								}
								if (currentStat == null) {
									return currentMetadata;
								}
								return new GeoWaveMetadata(
										currentMetadata.getPrimaryId(),
										currentMetadata.getSecondaryId(),
										vis,
										PersistenceUtils.toBinary(currentStat));
							}
						});
			}
			// convert to and from array just to avoid concurrent modification
			// issues on the iterator that is linked back to the metadataStore
			// sortedSet (basically clone the iterator, so for example deletes
			// can occur while iterating through this query result)
			return new CloseableIterator.Wrapper(
					Iterators.forArray(Iterators.toArray(
							itTransformed,
							GeoWaveMetadata.class)));
		}

	}

	private static final byte[] AMPRISAND = StringUtils.stringToBinary("&");

	private static byte[] combineVisibilities(
			final byte[] vis1,
			final byte[] vis2 ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}
		return ArrayUtils.addAll(
				ArrayUtils.addAll(
						vis1,
						AMPRISAND),
				vis2);
	}

	private static byte[] getNextPrefix(
			final byte[] bytes ) {
		if (bytes == null) {
			// this is simply for memory data store test purposes and is just an
			// attempt to go to the end of the memory datastore table
			return new byte[] {
				(byte) 0xFF,
				(byte) 0xFF,
				(byte) 0xFF,
				(byte) 0xFF,
				(byte) 0xFF,
				(byte) 0xFF,
				(byte) 0xFF,
			};
		}
		return new ByteArrayId(
				bytes).getNextPrefix();
	}

	private class MyMetadataWriter<T> implements
			MetadataWriter
	{
		private final MetadataType type;

		public MyMetadataWriter(
				final MetadataType type ) {
			super();
			this.type = type;
		}

		@Override
		public void close()
				throws IOException {}

		@Override
		public void flush() {
			try {
				close();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Error closing metadata writer",
						e);
			}
		}

		@Override
		public void write(
				final GeoWaveMetadata metadata ) {
			SortedSet<MemoryMetadataEntry> typeStore = metadataStore.get(type);
			if (typeStore == null) {
				typeStore = new TreeSet<>();
				metadataStore.put(
						type,
						typeStore);
			}
			if (typeStore.contains(new MemoryMetadataEntry(
					metadata))) {
				typeStore.remove(new MemoryMetadataEntry(
						metadata));
			}
			if (!typeStore.add(new MemoryMetadataEntry(
					metadata))) {
				LOGGER.warn("Unable to add new metadata");
			}

		}
	}

	private class MyMetadataDeleter extends
			MyMetadataReader implements
			MetadataDeleter
	{
		public MyMetadataDeleter(
				final MetadataType type ) {
			super(
					type);
		}

		@Override
		public void close()
				throws Exception {}

		@Override
		public boolean delete(
				final MetadataQuery query ) {
			try (CloseableIterator<GeoWaveMetadata> it = query(query)) {
				while (it.hasNext()) {
					it.next();
					it.remove();
				}
			}
			catch (final IOException e) {
				LOGGER.error(
						"Error deleting by query",
						e);
			}
			return true;
		}

		@Override
		public void flush() {}
	}

	public static class MemoryMetadataEntry implements
			Comparable<MemoryMetadataEntry>
	{
		private final GeoWaveMetadata metadata;
		// this is just to allow storing duplicates in the treemap
		private final byte[] uuidBytes;

		public MemoryMetadataEntry(
				final GeoWaveMetadata metadata ) {
			this(
					metadata,
					UUID.randomUUID().toString().getBytes(
							StringUtils.GEOWAVE_CHAR_SET));
		}

		public MemoryMetadataEntry(
				final GeoWaveMetadata metadata,
				final byte[] uuidBytes ) {
			this.metadata = metadata;
			this.uuidBytes = uuidBytes;
		}

		public GeoWaveMetadata getMetadata() {
			return metadata;
		}

		@Override
		public int compareTo(
				final MemoryMetadataEntry other ) {
			final Comparator<byte[]> lexyWithNullHandling = Ordering.from(
					UnsignedBytes.lexicographicalComparator()).nullsFirst();
			final int primaryIdCompare = lexyWithNullHandling.compare(
					metadata.getPrimaryId(),
					other.metadata.getPrimaryId());
			if (primaryIdCompare != 0) {
				return primaryIdCompare;
			}
			final int secondaryIdCompare = lexyWithNullHandling.compare(
					metadata.getSecondaryId(),
					other.metadata.getSecondaryId());
			if (secondaryIdCompare != 0) {
				return secondaryIdCompare;
			}
			final int visibilityCompare = lexyWithNullHandling.compare(
					metadata.getVisibility(),
					other.metadata.getVisibility());
			if (visibilityCompare != 0) {
				return visibilityCompare;
			}
			// this is just to allow storing duplicates in the treemap
			return lexyWithNullHandling.compare(
					uuidBytes,
					other.uuidBytes);

		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((metadata == null) ? 0 : metadata.hashCode());
			result = (prime * result) + Arrays.hashCode(uuidBytes);
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final MemoryMetadataEntry other = (MemoryMetadataEntry) obj;
			if (metadata == null) {
				if (other.metadata != null) {
					return false;
				}
			}
			return compareTo(other) == 0;
		}
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		// considering memory data store is for test purposes, this
		// implementation is unnecessary
		return true;
	}

	@Override
	public boolean metadataExists(
			final MetadataType type )
			throws IOException {
		return true;
	}
}
