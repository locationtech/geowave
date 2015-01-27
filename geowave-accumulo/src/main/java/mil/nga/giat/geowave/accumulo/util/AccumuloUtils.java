package mil.nga.giat.geowave.accumulo.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeSet;

import mil.nga.giat.geowave.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.accumulo.Writer;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.IngestEntryInfo;
import mil.nga.giat.geowave.store.IngestEntryInfo.FieldInfo;
import mil.nga.giat.geowave.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.DataWriter;
import mil.nga.giat.geowave.store.data.PersistentDataset;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A set of convenience methods for common operations on Accumulo within
 * GeoWave, such as conversions between GeoWave objects and corresponding
 * Accumulo objects.
 *
 */
public class AccumuloUtils
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloUtils.class);
	public final static String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static final UniformVisibilityWriter DEFAULT_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	public static Range byteArrayRangeToAccumuloRange(
			final ByteArrayRange byteArrayRange ) {
		final Text start = new Text(
				byteArrayRange.getStart().getBytes());
		final Text end = new Text(
				byteArrayRange.getEnd().getBytes());
		if (start.compareTo(end) > 0) {
			return null;
		}
		return new Range(
				new Text(
						byteArrayRange.getStart().getBytes()),
				true,
				Range.followingPrefix(new Text(
						byteArrayRange.getEnd().getBytes())),
				false);
	}

	public static TreeSet<Range> byteArrayRangesToAccumuloRanges(
			final List<ByteArrayRange> byteArrayRanges ) {
		if (byteArrayRanges == null) {
			final TreeSet<Range> range = new TreeSet<Range>();
			range.add(new Range());
			return range;
		}
		final TreeSet<Range> accumuloRanges = new TreeSet<Range>();
		for (final ByteArrayRange byteArrayRange : byteArrayRanges) {
			final Range range = byteArrayRangeToAccumuloRange(byteArrayRange);
			if (range == null) {
				continue;
			}
			accumuloRanges.add(range);
		}
		if (accumuloRanges.isEmpty()) {
			// implies full table scan
			accumuloRanges.add(new Range());
		}
		return accumuloRanges;
	}

	public static List<ByteArrayRange> constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
													// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(constraints);
		}
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

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_" + unqualifiedTableName;
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<?> adapter,
			final Index index ) {
		return decodeRow(
				key,
				value,
				adapter,
				null,
				index);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<?> adapter,
			final QueryFilter clientFilter,
			final Index index ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				key.getRow().copyBytes());
		return decodeRowObj(
				key,
				value,
				rowId,
				adapter,
				null,
				clientFilter,
				index);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				key.getRow().copyBytes());
		return decodeRowObj(
				key,
				value,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final AccumuloRowId rowId,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index ) {
		return decodeRowObj(
				key,
				value,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index);
	}

	private static <T> Object decodeRowObj(
			final Key key,
			final Value value,
			final AccumuloRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index ) {
		final Pair<T, IngestEntryInfo> pair = decodeRow(
				key,
				value,
				rowId,
				dataAdapter,
				adapterStore,
				clientFilter,
				index);
		return pair != null ? pair.getLeft() : null;

	}

	@SuppressWarnings("unchecked")
	public static <T> Pair<T, IngestEntryInfo> decodeRow(
			final Key k,
			final Value v,
			final AccumuloRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index ) {
		if ((dataAdapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		DataAdapter<T> adapter = dataAdapter;
		SortedMap<Key, Value> rowMapping;
		try {
			rowMapping = WholeRowIterator.decodeRow(
					k,
					v);
		}
		catch (final IOException e) {
			LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
			return null;
		}
		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
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
		final List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>(
				rowMapping.size());

		for (final Entry<Key, Value> entry : rowMapping.entrySet()) {
			// the column family is the data element's type ID
			if (adapterId == null) {
				adapterId = new ByteArrayId(
						entry.getKey().getColumnFamilyData().getBackingArray());
			}

			if (adapter == null) {
				adapter = (DataAdapter<T>) adapterStore.getAdapter(adapterId);
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
			final ByteArrayId fieldId = new ByteArrayId(
					entry.getKey().getColumnQualifierData().getBackingArray());
			// first check if this field is part of the index model
			final FieldReader<? extends CommonIndexValue> indexFieldReader = index.getIndexModel().getReader(
					fieldId);
			final byte byteValue[] = entry.getValue().get();
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(byteValue);
				indexValue.setVisibility(entry.getKey().getColumnVisibilityData().getBackingArray());
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				indexData.addValue(val);
				fieldInfoList.add(getFieldInfo(
						val,
						byteValue,
						indexValue.getVisibility()));
			}
			else {
				// next check if this field is part of the adapter's
				// extended data model
				final FieldReader<?> extFieldReader = adapter.getReader(fieldId);
				if (extFieldReader == null) {
					// if it still isn't resolved, log an error, and
					// continue
					LOGGER.error("field reader not found for data entry, the value will be ignored");
					continue;
				}
				final Object value = extFieldReader.readField(byteValue);
				final PersistentValue<Object> val = new PersistentValue<Object>(
						fieldId,
						value);
				extendedData.addValue(val);
				fieldInfoList.add(getFieldInfo(
						val,
						byteValue,
						entry.getKey().getColumnVisibility().getBytes()));
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
				extendedData);
		if ((clientFilter == null) || clientFilter.accept(encodedRow)) {
			// cannot get here unless adapter is found (not null)
			return Pair.of(
					adapter.decode(
							encodedRow,
							index),
					new IngestEntryInfo(
							Arrays.asList(new ByteArrayId(
									k.getRowData().getBackingArray())),
							fieldInfoList));
		}
		return null;
	}

	public static <T> IngestEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final Writer writer ) {
		return AccumuloUtils.write(
				writableAdapter,
				index,
				entry,
				writer,
				DEFAULT_VISIBILITY);
	}

	public static <T> IngestEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final Writer writer,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final IngestEntryInfo ingestInfo = getIngestInfo(
				writableAdapter,
				index,
				entry,
				customFieldVisibilityWriter);
		final List<Mutation> mutations = buildMutations(
				writableAdapter.getAdapterId().getBytes(),
				ingestInfo);

		writer.write(mutations);
		return ingestInfo;
	}

	public static <T> void removeFromAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final List<ByteArrayId> rowIds,
			final T entry,
			final Writer writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();

		final List<Mutation> mutations = new ArrayList<Mutation>();

		for (final ByteArrayId rowId : rowIds) {

			final Mutation mutation = new Mutation(
					new Text(
							dataId));
			mutation.putDelete(
					new Text(
							adapterId),
					new Text(
							rowId.getBytes()));

			mutations.add(mutation);
		}
		writer.write(mutations);
	}

	public static <T> void writeAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final IngestEntryInfo entryInfo,
			final T entry,
			final Writer writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();
		if ((dataId != null) && (dataId.length > 0)) {
			final List<Mutation> mutations = new ArrayList<Mutation>();

			for (final ByteArrayId rowId : entryInfo.getRowIds()) {

				final Mutation mutation = new Mutation(
						new Text(
								dataId));
				mutation.put(
						new Text(
								adapterId),
						new Text(
								rowId.getBytes()),
						new Value(
								"".getBytes()));

				mutations.add(mutation);
			}
			writer.write(mutations);
		}
	}

	public static <T> List<Mutation> entryToMutations(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final IngestEntryInfo ingestInfo = getIngestInfo(
				dataWriter,
				index,
				entry,
				customFieldVisibilityWriter);
		return buildMutations(
				dataWriter.getAdapterId().getBytes(),
				ingestInfo);
	}

	private static <T> List<Mutation> buildMutations(
			final byte[] adapterId,
			final IngestEntryInfo ingestInfo ) {
		final List<Mutation> mutations = new ArrayList<Mutation>();
		final List<FieldInfo> fieldInfoList = ingestInfo.getFieldInfo();
		for (final ByteArrayId rowId : ingestInfo.getRowIds()) {
			final Mutation mutation = new Mutation(
					new Text(
							rowId.getBytes()));
			for (final FieldInfo fieldInfo : fieldInfoList) {
				mutation.put(
						new Text(
								adapterId),
						new Text(
								fieldInfo.getDataValue().getId().getBytes()),
						new ColumnVisibility(
								fieldInfo.getVisibility()),
						new Value(
								fieldInfo.getWrittenValue()));
			}

			mutations.add(mutation);
		}
		return mutations;
	}

	/**
	 *
	 * @param dataWriter
	 * @param index
	 * @param entry
	 * @return List of zero or more matches
	 */
	public static <T> List<ByteArrayId> getRowIds(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry ) {
		final CommonIndexModel indexModel = index.getIndexModel();
		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());

		addToRowIds(
				rowIds,
				insertionIds,
				dataWriter.getDataId(
						entry).getBytes(),
				dataWriter.getAdapterId().getBytes(),
				encodedData.isDeduplicationEnabled());

		return rowIds;
	}

	private static <T> void addToRowIds(
			final List<ByteArrayId> rowIds,
			final List<ByteArrayId> insertionIds,
			final byte[] dataId,
			final byte[] adapterId,
			final boolean enableDeduplication ) {

		final int numberOfDuplicates = insertionIds.size() - 1;

		for (final ByteArrayId insertionId : insertionIds) {
			final byte[] indexId = insertionId.getBytes();
			// because the combination of the adapter ID and data ID
			// gaurantees uniqueness, we combine them in the row ID to
			// disambiguate index values that are the same, also adding
			// enough length values to be able to read the row ID again, we
			// lastly add a number of duplicates which can be useful as
			// metadata in our de-duplication
			// step
			rowIds.add(new ByteArrayId(
					new AccumuloRowId(
							indexId,
							dataId,
							adapterId,
							enableDeduplication ? numberOfDuplicates : -1).getRowId()));
		}
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> IngestEntryInfo getIngestInfo(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel = index.getIndexModel();
		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());
		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		final List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>();

		if (!insertionIds.isEmpty()) {
			addToRowIds(
					rowIds,
					insertionIds,
					dataWriter.getDataId(
							entry).getBytes(),
					dataWriter.getAdapterId().getBytes(),
					encodedData.isDeduplicationEnabled());

			for (final PersistentValue fieldValue : commonValues) {
				final FieldInfo<T> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue,
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(fieldInfo);
				}
			}
			for (final PersistentValue fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<T> fieldInfo = getFieldInfo(
							dataWriter,
							fieldValue,
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(fieldInfo);
					}
				}
			}
			return new IngestEntryInfo(
					rowIds,
					fieldInfoList);
		}
		LOGGER.warn("Indexing failed to produce insertion ids; entry [" + dataWriter.getDataId(
				entry).getString() + "] not saved.");
		return new IngestEntryInfo(
				Collections.EMPTY_LIST,
				Collections.EMPTY_LIST);

	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> FieldInfo<T> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<T> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter.getFieldVisibilityHandler(fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			return new FieldInfo<T>(
					fieldValue,
					fieldWriter.writeField(value),
					merge(
							customVisibilityHandler.getVisibility(
									entry,
									fieldValue.getId(),
									value),
							fieldWriter.getVisibility(
									entry,
									fieldValue.getId(),
									value)));
		}
		else if (fieldValue.getValue() != null) {
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for " + fieldValue.getValue());
		}
		return null;
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

	private static final byte[] BEG_AND_BYTE = "&".getBytes();
	private static final byte[] END_AND_BYTE = ")".getBytes();

	private static byte[] merge(
			final byte vis1[],
			final byte vis2[] ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		else if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}

		final ByteBuffer buffer = ByteBuffer.allocate(vis1.length + 3 + vis2.length);
		buffer.putChar('(');
		buffer.put(vis1);
		buffer.putChar(')');
		buffer.put(BEG_AND_BYTE);
		buffer.put(vis2);
		buffer.put(END_AND_BYTE);
		return buffer.array();
	}
}
