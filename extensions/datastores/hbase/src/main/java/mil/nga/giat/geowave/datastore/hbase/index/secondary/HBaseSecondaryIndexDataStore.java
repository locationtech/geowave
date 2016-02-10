package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.Closable;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class HBaseSecondaryIndexDataStore implements
		SecondaryIndexDataStore,
		Closable
{
	private final static Logger LOGGER = Logger.getLogger(HBaseSecondaryIndexDataStore.class);
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private final BasicHBaseOperations hbaseOperations;
	private final HBaseOptions hbaseOptions;
	private final Map<String, HBaseWriter> writerCache = new HashMap<>();

	public HBaseSecondaryIndexDataStore(
			final BasicHBaseOperations hbaseOperations ) {
		this(
				hbaseOperations,
				new HBaseOptions());
	}

	public HBaseSecondaryIndexDataStore(
			final BasicHBaseOperations hbaseOperations,
			final HBaseOptions hbaseOptions ) {
		super();
		this.hbaseOperations = hbaseOperations;
		this.hbaseOptions = hbaseOptions;
	}

	// TODO
//	private HBaseWriter getWriter(
//			final SecondaryIndex<?> secondaryIndex ) {
//		final String secondaryIndexName = secondaryIndex.getIndexStrategy().getId();
//		if (writerCache.containsKey(secondaryIndexName)) {
//			return writerCache.get(secondaryIndexName);
//		}
//		HBaseWriter writer = null;
//		try {
//			writer = hbaseOperations.createWriter(
//					TABLE_PREFIX + secondaryIndexName,
//					true,
//					false,
//					hbaseOptions.isEnableBlockCache(),
//					secondaryIndex.getIndexStrategy().getNaturalSplits());
//			writerCache.put(
//					secondaryIndexName,
//					writer);
//		}
//		catch (final TableNotFoundException e) {
//			LOGGER.error(
//					"Error creating writer",
//					e);
//		}
//		return writer;
//	}

	@Override
	public void store(
			final SecondaryIndex<?> secondaryIndex,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId,
			final List<FieldInfo<?>> indexedAttributes ) {
		// TODO
//		final HBaseWriter writer = getWriter(secondaryIndex);
//		if (writer != null) {
//			for (final FieldInfo<?> indexedAttribute : indexedAttributes) {
//				@SuppressWarnings("unchecked")
//				final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
//						Arrays.asList(indexedAttribute));
//				for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
//					writer.write(buildMutation(
//							insertionId.getBytes(),
//							secondaryIndex.getId().getBytes(),
//							indexedAttribute.getDataValue().getId().getBytes(),
//							indexedAttribute.getWrittenValue(),
//							indexedAttribute.getVisibility(),
//							primaryIndexId.getBytes(),
//							primaryIndexRowId.getBytes()));
//				}
//			}
//		}
	}

	@Override
	public void delete(
			final SecondaryIndex<?> secondaryIndex,
			final List<FieldInfo<?>> indexedAttributes ) {
		// TODO
//		final HBaseWriter writer = getWriter(secondaryIndex);
//		if (writer != null) {
//			for (final FieldInfo<?> indexedAttribute : indexedAttributes) {
//				@SuppressWarnings("unchecked")
//				final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
//						Arrays.asList(indexedAttribute));
//				for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
//					writer.write(buildDeleteMutation(
//							insertionId.getBytes(),
//							secondaryIndex.getId().getBytes(),
//							indexedAttribute.getDataValue().getId().getBytes()));
//				}
//			}
//		}
	}

	// TODO
//	private Mutation buildMutation(
//			final byte[] secondaryIndexRowId,
//			final byte[] secondaryIndexId,
//			final byte[] attributeName,
//			final byte[] attributeValue,
//			final byte[] visibility,
//			final byte[] primaryIndexId,
//			final byte[] primaryIndexRowId ) {
//		final Mutation m = new Mutation(
//				secondaryIndexRowId);
//		final ColumnVisibility columnVisibility = new ColumnVisibility(
//				visibility);
//		m.put(
//				secondaryIndexId,
//				attributeName,
//				columnVisibility,
//				attributeValue);
//		m.put(
//				secondaryIndexId,
//				primaryIndexId,
//				columnVisibility,
//				primaryIndexRowId);
//		return m;
//	}
//
//	private Mutation buildDeleteMutation(
//			final byte[] secondaryIndexRowId,
//			final byte[] secondaryIndexId,
//			final byte[] attributeName ) {
//		final Mutation m = new Mutation(
//				secondaryIndexRowId);
//		m.putDelete(
//				secondaryIndexId,
//				attributeName);
//		return m;
//	}

	@Override
	public CloseableIterator<ByteArrayId> query(
			final SecondaryIndex<?> secondaryIndex,
			final List<ByteArrayRange> ranges,
			final List<DistributableQueryFilter> constraints,
			final ByteArrayId primaryIndexId,
			final String... visibility ) {
		// TODO
		return null;
//		final Scanner scanner = getScanner(
//				secondaryIndex.getIndexStrategy().getId(),
//				visibility);
//		if (scanner != null) {
//			final Collection<ByteArrayId> primaryIndexRowIds = new ArrayList<>();
//			scanner.addScanIterator(getScanIteratorSettings(
//					constraints,
//					primaryIndexId));
//			final Collection<Range> scanRanges = getScanRanges(ranges);
//			for (final Range range : scanRanges) {
//				scanner.setRange(range);
//				for (final Entry<Key, Value> entry : scanner) {
//					if (entry.getKey().getColumnQualifier().toString().equals(
//							primaryIndexId.getString())) {
//						// found query match: keep track of primaryIndexRowId
//						primaryIndexRowIds.add(new ByteArrayId(
//								entry.getValue().get()));
//					}
//				}
//			}
//			return new CloseableIteratorWrapper<ByteArrayId>(
//					new Closeable() {
//						@Override
//						public void close()
//								throws IOException {
//							scanner.close();
//						}
//					},
//					primaryIndexRowIds.iterator());
//		}
//		return new CloseableIterator.Empty<ByteArrayId>();
	}

	// TODO
//	private Scanner getScanner(
//			final String secondaryIndexId,
//			final String... visibility ) {
//		Scanner scanner = null;
//		try {
//			scanner = hbaseOperations.createScanner(
//					TABLE_PREFIX + secondaryIndexId,
//					visibility);
//		}
//		catch (final TableNotFoundException e) {
//			LOGGER.error(
//					"Could not obtain batch scanner",
//					e);
//		}
//		return scanner;
//	}
//
//	private Collection<Range> getScanRanges(
//			final List<ByteArrayRange> ranges ) {
//		final Collection<Range> scanRanges = new ArrayList<>();
//		for (final ByteArrayRange range : ranges) {
//			scanRanges.add(new Range(
//					new Text(
//							range.getStart().getBytes()),
//					new Text(
//							range.getEnd().getBytes())));
//		}
//		return scanRanges;
//	}
//
//	private IteratorSetting getScanIteratorSettings(
//			final List<DistributableQueryFilter> distributableFilters,
//			final ByteArrayId primaryIndexId ) {
//		final DistributableFilterList filterList = new DistributableFilterList(
//				false,
//				distributableFilters);
//		final IteratorSetting iteratorSettings = new IteratorSetting(
//				SecondaryIndexQueryFilterIterator.ITERATOR_PRIORITY,
//				SecondaryIndexQueryFilterIterator.ITERATOR_NAME,
//				SecondaryIndexQueryFilterIterator.class);
//		iteratorSettings.addOption(
//				SecondaryIndexQueryFilterIterator.FILTERS,
//				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filterList)));
//		iteratorSettings.addOption(
//				SecondaryIndexQueryFilterIterator.PRIMARY_INDEX_ID,
//				primaryIndexId.getString());
//		return iteratorSettings;
//	}

	@Override
	public void close() {
		// TODO
//		for (final Writer writer : writerCache.values()) {
//			writer.close();
//		}
	}

	@Override
	public void flush() {
		close();
	}
}