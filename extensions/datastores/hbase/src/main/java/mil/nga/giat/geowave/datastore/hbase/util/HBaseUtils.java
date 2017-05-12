package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;

@SuppressWarnings("rawtypes")
public class HBaseUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);

	// we append a 0 byte, 8 bytes of timestamp, and 16 bytes of UUID when
	// needed for uniqueness
	public final static int UNIQUE_ADDED_BYTES = 1 + 8 + 16;
	public final static byte UNIQUE_ID_DELIMITER = 0;
	private static final UniformVisibilityWriter DEFAULT_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	private static <T> List<RowMutations> buildMutations(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo,
			final PrimaryIndex index,
			final WritableDataAdapter<T> writableAdapter,
			final boolean ensureUniqueId ) {
		final List<RowMutations> mutations = new ArrayList<RowMutations>();
		final List<FieldInfo<?>> fieldInfoList = DataStoreUtils.composeFlattenedFields(
				ingestInfo.getFieldInfo(),
				index.getIndexModel(),
				writableAdapter);

		for (ByteArrayId rowId : ingestInfo.getRowIds()) {
			if (ensureUniqueId) {
				rowId = ensureUniqueId(
						rowId.getBytes(),
						true);
			}
			final RowMutations mutation = new RowMutations(
					rowId.getBytes());
			try {
				final Put row = new Put(
						rowId.getBytes());
				for (final FieldInfo fieldInfo : fieldInfoList) {
					row.addColumn(
							adapterId,
							fieldInfo.getDataValue().getId().getBytes(),
							fieldInfo.getWrittenValue());
				}
				mutation.add(row);
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Could not add row to mutation.",
						e);
			}
			mutations.add(mutation);
		}

		return mutations;
	}

	// Retrieves the next incremental HBase prefix following the passed-in
	// prefix
	// Using a private HBase method called from the constructor of Scan
	public static byte[] getNextPrefix(
			final byte[] prefix ) {
		return new Scan().setRowPrefixFilter(
				prefix).getStopRow();
	}

	public static <T> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index,
			final T entry,
			final HBaseWriter writer,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				writableAdapter,
				index,
				entry,
				customFieldVisibilityWriter);

		final List<RowMutations> mutations = buildMutations(
				writableAdapter.getAdapterId().getBytes(),
				ingestInfo,
				index,
				writableAdapter,
				(writableAdapter instanceof RowMergingDataAdapter)
						&& (((RowMergingDataAdapter) writableAdapter).getTransform() != null));

		try {
			writer.write(
					mutations,
					writableAdapter.getAdapterId().getString());
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Writing to table failed.",
					e);
		}

		return ingestInfo;
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_"
				+ unqualifiedTableName;
	}

	public static <T> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index,
			final T entry,
			final HBaseWriter writer ) {
		return write(
				writableAdapter,
				index,
				entry,
				writer,
				DEFAULT_VISIBILITY);
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> FieldInfo<T> getFieldInfo(
			final PersistentValue<T> fieldValue,
			final byte[] value,
			final byte[] visibility ) {
		return new FieldInfo<T>(
				fieldValue,
				value,
				visibility);
	}

	public static List<ByteArrayRange> constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
			// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(
					constraints,
					maxRanges);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T decodeRow(
			final Result row,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback,
			final byte[] fieldSubsetBitmask,
			boolean decodeRow ) {

		final GeowaveRowId rowId = new GeowaveRowId(
				row.getRow());
		return (T) decodeRowObj(
				row,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				scanCallback,
				fieldSubsetBitmask,
				decodeRow);
	}

	public static Object decodeRow(
			final Result row,
			final GeowaveRowId rowId,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			boolean decodeRow ) {
		return decodeRowObj(
				row,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				null,
				null,
				decodeRow);
	}

	private static Object decodeRowObj(
			final Result row,
			final GeowaveRowId rowId,
			final DataAdapter dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback scanCallback,
			final byte[] fieldSubsetBitmask,
			boolean decodeRow ) {
		final Pair<Object, DataStoreEntryInfo> pair = decodeRow(
				row,
				rowId,
				dataAdapter,
				adapterStore,
				clientFilter,
				index,
				scanCallback,
				fieldSubsetBitmask,
				decodeRow);
		return pair != null ? pair.getLeft() : null;
	}

	@SuppressWarnings("unchecked")
	public static Pair<Object, DataStoreEntryInfo> decodeRow(
			final Result row,
			final GeowaveRowId rowId,
			final DataAdapter dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback scanCallback,
			final byte[] fieldSubsetBitmask,
			boolean decodeRow ) {
		if ((dataAdapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		DataAdapter adapter = dataAdapter;

		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();

		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();

		// for now we are assuming all entries in a row are of the same type
		// and use the same adapter
		boolean adapterMatchVerified;
		ByteArrayId adapterId;
		if (adapter != null) {
			adapterId = adapter.getAdapterId();
			adapterMatchVerified = false;
		}
		else {
			adapterMatchVerified = true;
			adapterId = null;
		}
		final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = row.getMap();
		final List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		for (final Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cfEntry : map.entrySet()) {
			// the column family is the data element's type ID
			if (adapterId == null) {
				adapterId = new ByteArrayId(
						cfEntry.getKey());
			}

			if (adapter == null) {
				adapter = adapterStore.getAdapter(adapterId);
				if (adapter == null) {
					LOGGER.error("DataAdapter does not exist");
					return null;
				}
			}
			if (!adapterMatchVerified) {
				if (!adapterId.equals(adapter.getAdapterId())) {
					return null;
				}
				adapterMatchVerified = true;
			}
			for (final Entry<byte[], NavigableMap<Long, byte[]>> cqEntry : cfEntry.getValue().entrySet()) {
				final CommonIndexModel indexModel = index.getIndexModel();
				byte[] byteValue = cqEntry.getValue().lastEntry().getValue();
				byte[] qualifier = cqEntry.getKey();

				if (fieldSubsetBitmask != null) {
					final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
							qualifier,
							fieldSubsetBitmask);
					byteValue = BitmaskUtils.constructNewValue(
							byteValue,
							qualifier,
							newBitmask);
					qualifier = newBitmask;
				}

				DataStoreUtils.readFieldInfo(
						fieldInfoList,
						indexData,
						extendedData,
						unknownData,
						qualifier,
						new byte[] {},
						byteValue,
						adapter,
						indexModel);
			}
		}

		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				adapterId,
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				indexData,
				unknownData,
				extendedData);

		if ((clientFilter == null) || clientFilter.accept(
				index.getIndexModel(),
				encodedRow)) {
			// cannot get here unless adapter is found (not null)
			if (adapter == null) {
				LOGGER.error("Error, adapter was null when it should not be");
			}
			else {
				final Pair<Object, DataStoreEntryInfo> pair = Pair.of(
						decodeRow ? adapter.decode(
								encodedRow,
								index) : encodedRow,
						new DataStoreEntryInfo(
								rowId.getDataId(),
								Arrays.asList(new ByteArrayId(
										rowId.getInsertionId())),
								Arrays.asList(new ByteArrayId(
										row.getRow())),
								fieldInfoList));
				if (scanCallback != null && decodeRow) {
					scanCallback.entryScanned(
							pair.getRight(),
							pair.getLeft());
				}
				return pair;
			}
		}
		return null;
	}

	public static <T> void writeAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final DataStoreEntryInfo entryInfo,
			final T entry,
			final HBaseWriter writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();
		if ((dataId != null) && (dataId.length > 0)) {
			final List<RowMutations> mutations = new ArrayList<RowMutations>();

			for (final ByteArrayId rowId : entryInfo.getRowIds()) {
				final RowMutations mutation = new RowMutations(
						rowId.getBytes());

				try {
					final Put row = new Put(
							rowId.getBytes());
					row.addColumn(
							adapterId,
							rowId.getBytes(),
							"".getBytes(StringUtils.UTF8_CHAR_SET));
					mutation.add(row);
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Could not add row to mutation.",
							e);
				}
				mutations.add(mutation);
			}
			try {
				writer.write(
						mutations,
						writableAdapter.getAdapterId().getString());
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Writing to table failed.",
						e);
			}

		}
	}

	public static RowMutations getDeleteMutations(
			final byte[] rowId,
			final byte[] columnFamily,
			final byte[] columnQualifier,
			final String[] authorizations )
			throws IOException {
		final RowMutations m = new RowMutations(
				rowId);
		final Delete d = new Delete(
				rowId);
		d.addColumns(
				columnFamily,
				columnQualifier);
		m.add(d);
		return m;
	}

	public static class ScannerClosableWrapper implements
			Closeable
	{
		private final ResultScanner results;

		public ScannerClosableWrapper(
				final ResultScanner results ) {
			this.results = results;
		}

		@Override
		public void close() {
			results.close();
		}

	}

	public static class MultiScannerClosableWrapper implements
			Closeable
	{
		private final List<ResultScanner> results;

		public MultiScannerClosableWrapper(
				final List<ResultScanner> results ) {
			this.results = results;
		}

		@Override
		public void close() {
			for (final ResultScanner scanner : results) {
				scanner.close();
			}
		}

	}

	public static ByteArrayId ensureUniqueId(
			final byte[] id,
			final boolean hasMetadata ) {

		final ByteBuffer buf = ByteBuffer.allocate(id.length + UNIQUE_ADDED_BYTES);

		byte[] metadata = null;
		byte[] data;
		if (hasMetadata) {
			metadata = Arrays.copyOfRange(
					id,
					id.length - 12,
					id.length);

			final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
			final int adapterIdLength = metadataBuf.getInt();
			int dataIdLength = metadataBuf.getInt();
			dataIdLength += UNIQUE_ADDED_BYTES;
			final int duplicates = metadataBuf.getInt();

			final ByteBuffer newMetaData = ByteBuffer.allocate(metadata.length);
			newMetaData.putInt(adapterIdLength);
			newMetaData.putInt(dataIdLength);
			newMetaData.putInt(duplicates);

			metadata = newMetaData.array();

			data = Arrays.copyOfRange(
					id,
					0,
					id.length - 12);
		}
		else {
			data = id;
		}

		buf.put(data);

		final long timestamp = System.nanoTime();
		final UUID uuid = UUID.randomUUID();
		buf.put(new byte[] {
			UNIQUE_ID_DELIMITER
		});
		buf.putLong(timestamp);
		buf.putLong(uuid.getMostSignificantBits());
		buf.putLong(uuid.getLeastSignificantBits());

		if (hasMetadata) {
			buf.put(metadata);
		}

		return new ByteArrayId(
				buf.array());
	}

	public static boolean rowIdsMatch(
			final GeowaveRowId rowId1,
			final GeowaveRowId rowId2 ) {

		if (!Arrays.equals(
				rowId1.getAdapterId(),
				rowId2.getAdapterId())) {
			return false;
		}

		if (Arrays.equals(
				rowId1.getDataId(),
				rowId2.getDataId())) {
			return true;
		}

		return Arrays.equals(
				removeUniqueId(rowId1.getRowId()),
				removeUniqueId(rowId2.getRowId()));
	}

	public static byte[] removeUniqueId(
			final byte[] row ) {

		final GeowaveRowId rowId = new GeowaveRowId(
				row);
		byte[] dataId = rowId.getDataId();

		if ((dataId.length < UNIQUE_ADDED_BYTES) || (dataId[dataId.length - UNIQUE_ADDED_BYTES] != UNIQUE_ID_DELIMITER)) {
			return row;
		}

		dataId = Arrays.copyOfRange(
				dataId,
				0,
				dataId.length - UNIQUE_ADDED_BYTES);

		return new GeowaveRowId(
				rowId.getInsertionId(),
				dataId,
				rowId.getAdapterId(),
				rowId.getNumberOfDuplicates()).getRowId();
	}
}