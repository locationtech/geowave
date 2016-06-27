package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig.OptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingAdapterOptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingVisibilityCombiner;
import mil.nga.giat.geowave.datastore.accumulo.Writer;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloDataSet;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloFieldInfo;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloUnreadDataSingleRow;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;

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
	private static final String ROW_MERGING_SUFFIX = "_COMBINER";
	private static final String ROW_MERGING_VISIBILITY_SUFFIX = "_VISIBILITY_COMBINER";

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

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_"
				+ unqualifiedTableName;
	}

	@SuppressWarnings("unchecked")
	public static <T> T decodeRow(
			final Key key,
			final Value value,
			final boolean wholeRowEncoding,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				key.getRow().copyBytes());
		return (T) decodeRowObj(
				key,
				value,
				wholeRowEncoding,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final boolean wholeRowEncoding,
			final AccumuloRowId rowId,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index ) {
		return decodeRowObj(
				key,
				value,
				wholeRowEncoding,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				null);
	}

	private static <T> Object decodeRowObj(
			final Key key,
			final Value value,
			final boolean wholeRowEncoding,
			final AccumuloRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback ) {
		final Pair<T, DataStoreEntryInfo> pair = decodeRow(
				key,
				value,
				wholeRowEncoding,
				rowId,
				dataAdapter,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
		return pair != null ? pair.getLeft() : null;

	}

	@SuppressWarnings("unchecked")
	public static <T> Pair<T, DataStoreEntryInfo> decodeRow(
			final Key k,
			final Value v,
			final boolean wholeRowEncoding,
			final AccumuloRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback ) {
		if ((dataAdapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		DataAdapter<T> adapter = dataAdapter;
		SortedMap<Key, Value> rowMapping;
		if (wholeRowEncoding) {
			try {
				rowMapping = WholeRowIterator.decodeRow(
						k,
						v);
			}
			catch (final IOException e) {
				LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
				return null;
			}
		}
		else {
			rowMapping = new TreeMap<Key, Value>();
			rowMapping.put(
					k,
					v);
		}
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
		final List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>(
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
			final CommonIndexModel indexModel = index.getIndexModel();
			final byte byteValue[] = entry.getValue().get();
			final ByteArrayId compositeFieldId = new ByteArrayId(
					entry.getKey().getColumnQualifierData().getBackingArray());
			final List<AccumuloFieldInfo> fieldInfos = decomposeFlattenedFields(
					compositeFieldId.getBytes(),
					byteValue,
					entry.getKey().getColumnVisibilityData().getBackingArray(),
					-1).getFieldsRead();
			for (final AccumuloFieldInfo fieldInfo : fieldInfos) {
				final ByteArrayId fieldId = adapter.getFieldIdForPosition(
						indexModel,
						fieldInfo.getFieldPosition());
				final FieldReader<? extends CommonIndexValue> indexFieldReader = indexModel.getReader(fieldId);
				if (indexFieldReader != null) {
					final CommonIndexValue indexValue = indexFieldReader.readField(fieldInfo.getValue());
					indexValue.setVisibility(entry.getKey().getColumnVisibilityData().getBackingArray());
					final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
							fieldId,
							indexValue);
					indexData.addValue(val);
					fieldInfoList.add(DataStoreUtils.getFieldInfo(
							val,
							fieldInfo.getValue(),
							entry.getKey().getColumnVisibilityData().getBackingArray()));
				}
				else {
					final FieldReader<?> extFieldReader = adapter.getReader(fieldId);
					if (extFieldReader != null) {
						final Object value = extFieldReader.readField(fieldInfo.getValue());
						final PersistentValue<Object> val = new PersistentValue<Object>(
								fieldId,
								value);
						extendedData.addValue(val);
						fieldInfoList.add(DataStoreUtils.getFieldInfo(
								val,
								fieldInfo.getValue(),
								entry.getKey().getColumnVisibilityData().getBackingArray()));
					}
					else {
						LOGGER.error("field reader not found for data entry, the value may be ignored");
						unknownData.addValue(new PersistentValue<byte[]>(
								fieldId,
								fieldInfo.getValue()));
					}
				}
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
				final Pair<T, DataStoreEntryInfo> pair = Pair.of(
						adapter.decode(
								encodedRow,
								index),
						new DataStoreEntryInfo(
								rowId.getDataId(),
								Arrays.asList(new ByteArrayId(
										k.getRowData().getBackingArray())),
								fieldInfoList));
				if (scanCallback != null) {
					scanCallback.entryScanned(
							pair.getRight(),
							pair.getLeft());
				}
				return pair;
			}
		}
		return null;
	}

	/**
	 *
	 * Takes a byte array representing a serialized composite group of
	 * FieldInfos sharing a common visibility and returns a List of the
	 * individual FieldInfos
	 *
	 * @param compositeFieldId
	 *            the composite bitmask representing the fields contained within
	 *            the flattenedValue
	 * @param flattenedValue
	 *            the serialized composite FieldInfo
	 * @param commonVisibility
	 *            the shared visibility
	 * @param maxFieldPosition
	 *            can short-circuit read and defer decomposition of fields after
	 *            a given position
	 * @return the dataset that has been read
	 */
	public static <T> AccumuloDataSet decomposeFlattenedFields(
			final byte[] bitmask,
			final byte[] flattenedValue,
			final byte[] commonVisibility,
			final int maxFieldPosition ) {
		final List<AccumuloFieldInfo> fieldInfoList = new ArrayList<AccumuloFieldInfo>();
		final List<Integer> fieldPositions = BitmaskUtils.getFieldPositions(bitmask);

		final boolean sharedVisibility = fieldPositions.size() > 1;
		if (sharedVisibility) {
			final ByteBuffer input = ByteBuffer.wrap(flattenedValue);
			for (int i = 0; i < fieldPositions.size(); i++) {
				final Integer fieldPosition = fieldPositions.get(i);
				if ((maxFieldPosition > -1) && (fieldPosition > maxFieldPosition)) {
					return new AccumuloDataSet(
							fieldInfoList,
							new AccumuloUnreadDataSingleRow(
									input,
									i,
									fieldPositions));
				}
				final int fieldLength = input.getInt();
				final byte[] fieldValueBytes = new byte[fieldLength];
				input.get(fieldValueBytes);
				fieldInfoList.add(new AccumuloFieldInfo(
						fieldPosition,
						fieldValueBytes));
			}
		}
		else {
			fieldInfoList.add(new AccumuloFieldInfo(
					fieldPositions.get(0),
					flattenedValue));

		}
		return new AccumuloDataSet(
				fieldInfoList,
				null);
	}

	public static <T> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index,
			final T entry,
			final Writer writer,
			final AccumuloOperations operations,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		// we need to make sure at least this user has authorization
		// on the visibility that is being written
		try {
			final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
					writableAdapter,
					index,
					entry,
					customFieldVisibilityWriter);
			if (customFieldVisibilityWriter != DataStoreUtils.UNCONSTRAINED_VISIBILITY) {
				for (final FieldInfo field : ingestInfo.getFieldInfo()) {
					if ((field.getVisibility() != null) && (field.getVisibility().length > 0)) {
						operations.insureAuthorization(
								null,
								StringUtils.stringFromBinary(field.getVisibility()));

					}
				}
			}
			final List<Mutation> mutations = buildMutations(
					writableAdapter.getAdapterId().getBytes(),
					ingestInfo,
					index,
					writableAdapter);

			writer.write(mutations);
			return ingestInfo;
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to add user authorization",
					e);
		}
		return null;
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
			final DataStoreEntryInfo entryInfo,
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
								"".getBytes(StringUtils.GEOWAVE_CHAR_SET)));

				mutations.add(mutation);
			}
			writer.write(mutations);
		}
	}

	public static <T> List<Mutation> entryToMutations(
			final WritableDataAdapter<T> dataWriter,
			final PrimaryIndex index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				dataWriter,
				index,
				entry,
				customFieldVisibilityWriter);
		return buildMutations(
				dataWriter.getAdapterId().getBytes(),
				ingestInfo,
				index,
				dataWriter);
	}

	private static <T> List<Mutation> buildMutations(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo,
			final PrimaryIndex index,
			final WritableDataAdapter<T> writableAdapter ) {
		final List<Mutation> mutations = new ArrayList<Mutation>();
		final List<FieldInfo<?>> fieldInfoList = composeFlattenedFields(
				ingestInfo.getFieldInfo(),
				index.getIndexModel(),
				writableAdapter);
		for (final ByteArrayId rowId : ingestInfo.getRowIds()) {
			final Mutation mutation = new Mutation(
					new Text(
							rowId.getBytes()));
			for (final FieldInfo<?> fieldInfo : fieldInfoList) {
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
	 * This method combines all FieldInfos that share a common visibility into a
	 * single FieldInfo
	 *
	 * @param originalList
	 * @return a new list of composite FieldInfos
	 */
	private static <T> List<FieldInfo<?>> composeFlattenedFields(
			final List<FieldInfo<?>> originalList,
			final CommonIndexModel model,
			final WritableDataAdapter<?> writableAdapter ) {
		final List<FieldInfo<?>> retVal = new ArrayList<>();
		final Map<ByteArrayId, List<Pair<Integer, FieldInfo<?>>>> vizToFieldMap = new LinkedHashMap<>();
		boolean sharedVisibility = false;
		// organize FieldInfos by unique visibility
		for (final FieldInfo<?> fieldInfo : originalList) {
			final int fieldPosition = writableAdapter.getPositionOfOrderedField(
					model,
					fieldInfo.getDataValue().getId());
			if (fieldPosition == -1) {
				writableAdapter.getPositionOfOrderedField(
						model,
						fieldInfo.getDataValue().getId());
			}
			final ByteArrayId currViz = new ByteArrayId(
					fieldInfo.getVisibility());
			if (vizToFieldMap.containsKey(currViz)) {
				sharedVisibility = true;
				final List<Pair<Integer, FieldInfo<?>>> listForViz = vizToFieldMap.get(currViz);
				listForViz.add(new ImmutablePair<Integer, DataStoreEntryInfo.FieldInfo<?>>(
						fieldPosition,
						fieldInfo));
			}
			else {
				final List<Pair<Integer, FieldInfo<?>>> listForViz = new ArrayList<>();
				listForViz.add(new ImmutablePair<Integer, DataStoreEntryInfo.FieldInfo<?>>(
						fieldPosition,
						fieldInfo));
				vizToFieldMap.put(
						currViz,
						listForViz);
			}
		}
		if (!sharedVisibility) {
			// at a minimum, must return transformed (bitmasked) fieldInfos
			final List<FieldInfo<?>> bitmaskedFieldInfos = new ArrayList<>();
			for (final List<Pair<Integer, FieldInfo<?>>> list : vizToFieldMap.values()) {
				// every list must have exactly one element
				final Pair<Integer, FieldInfo<?>> fieldInfo = list.get(0);
				bitmaskedFieldInfos.add(new FieldInfo<>(
						new PersistentValue<Object>(
								new ByteArrayId(
										BitmaskUtils.generateCompositeBitmask(fieldInfo.getLeft())),
								fieldInfo.getRight().getDataValue().getValue()),
						fieldInfo.getRight().getWrittenValue(),
						fieldInfo.getRight().getVisibility()));
			}
			return bitmaskedFieldInfos;
		}
		for (final Entry<ByteArrayId, List<Pair<Integer, FieldInfo<?>>>> entry : vizToFieldMap.entrySet()) {
			final List<byte[]> fieldInfoBytesList = new ArrayList<>();
			int totalLength = 0;
			final SortedSet<Integer> fieldPositions = new TreeSet<Integer>();
			final List<Pair<Integer, FieldInfo<?>>> fieldInfoList = entry.getValue();
			Collections.sort(
					fieldInfoList,
					new BitmaskedFieldInfoComparator());
			for (final Pair<Integer, FieldInfo<?>> fieldInfoPair : fieldInfoList) {
				final FieldInfo<?> fieldInfo = fieldInfoPair.getRight();
				final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(4 + fieldInfo.getWrittenValue().length);
				fieldPositions.add(fieldInfoPair.getLeft());
				fieldInfoBytes.putInt(fieldInfo.getWrittenValue().length);
				fieldInfoBytes.put(fieldInfo.getWrittenValue());
				fieldInfoBytesList.add(fieldInfoBytes.array());
				totalLength += fieldInfoBytes.array().length;
			}
			final ByteBuffer allFields = ByteBuffer.allocate(totalLength);
			for (final byte[] bytes : fieldInfoBytesList) {
				allFields.put(bytes);
			}
			final byte[] compositeBitmask = BitmaskUtils.generateCompositeBitmask(fieldPositions);
			final FieldInfo<?> composite = new FieldInfo<T>(
					new PersistentValue<T>(
							new ByteArrayId(
									compositeBitmask),
							null), // unnecessary
					allFields.array(),
					entry.getKey().getBytes());
			retVal.add(composite);
		}
		return retVal;
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
			final PrimaryIndex index,
			final T entry ) {
		final CommonIndexModel indexModel = index.getIndexModel();
		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());

		DataStoreUtils.addToRowIds(
				rowIds,
				insertionIds,
				dataWriter.getDataId(
						entry).getBytes(),
				dataWriter.getAdapterId().getBytes(),
				encodedData.isDeduplicationEnabled());

		return rowIds;
	}

	/**
	 * Get Namespaces
	 *
	 * @param connector
	 */
	public static List<String> getNamespaces(
			final Connector connector ) {
		final List<String> namespaces = new ArrayList<String>();

		for (final String table : connector.tableOperations().list()) {
			final int idx = table.indexOf(AbstractAccumuloPersistence.METADATA_TABLE) - 1;
			if (idx > 0) {
				namespaces.add(table.substring(
						0,
						idx));
			}
		}
		return namespaces;
	}

	/**
	 * Get list of data adapters associated with the given namespace
	 *
	 * @param connector
	 * @param namespace
	 */
	public static List<DataAdapter<?>> getDataAdapters(
			final Connector connector,
			final String namespace ) {
		final List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();

		final AdapterStore adapterStore = new AccumuloAdapterStore(
				new BasicAccumuloOperations(
						connector,
						namespace));

		try (final CloseableIterator<DataAdapter<?>> itr = adapterStore.getAdapters()) {

			while (itr.hasNext()) {
				adapters.add(itr.next());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}

		return adapters;
	}

	/**
	 * Get list of indices associated with the given namespace
	 *
	 * @param connector
	 * @param namespace
	 */
	public static List<Index<?, ?>> getIndices(
			final Connector connector,
			final String namespace ) {
		final List<Index<?, ?>> indices = new ArrayList<Index<?, ?>>();

		final IndexStore indexStore = new AccumuloIndexStore(
				new BasicAccumuloOperations(
						connector,
						namespace));

		try (final CloseableIterator<Index<?, ?>> itr = indexStore.getIndices()) {

			while (itr.hasNext()) {
				indices.add(itr.next());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}

		return indices;
	}

	/**
	 * Set splits on a table based on a partition ID
	 *
	 * @param namespace
	 * @param index
	 * @param randomParitions
	 *            number of partition IDs
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByRandomPartitions(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final int randomPartitions )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		operations.createTable(index.getId().getString());
		final RoundRobinKeyIndexStrategy partitions = new RoundRobinKeyIndexStrategy(
				randomPartitions);
		final SortedSet<Text> splits = new TreeSet<Text>();
		for (final ByteArrayId split : partitions.getNaturalSplits()) {
			splits.add(new Text(
					split.getBytes()));
		}

		final String tableName = AccumuloUtils.getQualifiedTableName(
				namespace,
				index.getId().getString());
		connector.tableOperations().addSplits(
				tableName,
				splits);
	}

	/**
	 * Set splits on a table based on quantile distribution and fixed number of
	 * splits
	 *
	 * @param namespace
	 * @param index
	 * @param quantile
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByQuantile(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final int quantile )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final long count = getEntries(
				connector,
				namespace,
				index);

		try (final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index)) {

			if (iterator == null) {
				LOGGER.error("Could not get iterator instance, getIterator returned null");
				throw new IOException(
						"Could not get iterator instance, getIterator returned null");
			}

			long ii = 0;
			final long splitInterval = (long) Math.ceil((double) count / (double) quantile);
			final SortedSet<Text> splits = new TreeSet<Text>();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				ii++;
				if (ii >= splitInterval) {
					ii = 0;
					splits.add(entry.getKey().getRow());
				}
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					index.getId().getString());
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
	}

	/**
	 * Set splits on table based on equal interval distribution and fixed number
	 * of splits.
	 *
	 * @param namespace
	 * @param index
	 * @param numberSplits
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByNumSplits(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final int numSplits )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final SortedSet<Text> splits = new TreeSet<Text>();

		try (final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index)) {

			if (iterator == null) {
				LOGGER.error("could not get iterator instance, getIterator returned null");
				throw new IOException(
						"could not get iterator instance, getIterator returned null");
			}

			final int numberSplits = numSplits - 1;
			BigInteger min = null;
			BigInteger max = null;

			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				final byte[] bytes = entry.getKey().getRow().getBytes();
				final BigInteger value = new BigInteger(
						bytes);
				if ((min == null) || (max == null)) {
					min = value;
					max = value;
				}
				min = min.min(value);
				max = max.max(value);
			}

			final BigDecimal dMax = new BigDecimal(
					max);
			final BigDecimal dMin = new BigDecimal(
					min);
			BigDecimal delta = dMax.subtract(dMin);
			delta = delta.divideToIntegralValue(new BigDecimal(
					numSplits));

			for (int ii = 1; ii <= numberSplits; ii++) {
				final BigDecimal temp = delta.multiply(BigDecimal.valueOf(ii));
				final BigInteger value = min.add(temp.toBigInteger());

				final Text split = new Text(
						value.toByteArray());
				splits.add(split);
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					StringUtils.stringFromBinary(index.getId().getBytes()));
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
	}

	/**
	 * Set splits on table based on fixed number of rows per split.
	 *
	 * @param namespace
	 * @param index
	 * @param numberRows
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByNumRows(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final long numberRows )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		try (final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index)) {

			if (iterator == null) {
				LOGGER.error("Unable to get iterator instance, getIterator returned null");
				throw new IOException(
						"Unable to get iterator instance, getIterator returned null");
			}

			long ii = 0;
			final SortedSet<Text> splits = new TreeSet<Text>();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				ii++;
				if (ii >= numberRows) {
					ii = 0;
					splits.add(entry.getKey().getRow());
				}
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					StringUtils.stringFromBinary(index.getId().getBytes()));
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
	}

	/**
	 * Check if locality group is set.
	 *
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static boolean isLocalityGroupSet(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		// get unqualified table name
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		return operations.localityGroupExists(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * Set locality group.
	 *
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setLocalityGroup(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		// get unqualified table name
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		operations.addLocalityGroup(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * Get number of entries for a data adapter in an index.
	 *
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);
		if (indexStore.indexExists(index.getId()) && adapterStore.adapterExists(adapter.getAdapterId())) {
			final List<ByteArrayId> adapterIds = new ArrayList<>();
			adapterIds.add(adapter.getAdapterId());
			final AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
					adapterIds,
					index,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					new String[0]);
			final CloseableIterator<?> iterator = accumuloQuery.query(
					operations,
					new AccumuloAdapterStore(
							operations),
					null,
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * * Get number of entries per index.
	 *
	 * @param namespace
	 * @param index
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		if (indexStore.indexExists(index.getId())) {
			final AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
					null,
					index,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					new String[0]);
			final CloseableIterator<?> iterator = accumuloQuery.query(
					operations,
					new AccumuloAdapterStore(
							operations),
					null,
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	public static void attachRowMergingIterators(
			final RowMergingDataAdapter<?, ?> adapter,
			final AccumuloOperations operations,
			final String tableName,
			final boolean createTable )
			throws TableNotFoundException {
		final EnumSet<IteratorScope> visibilityCombinerScope = EnumSet.of(IteratorScope.scan);
		final OptionProvider optionProvider = new RowMergingAdapterOptionProvider(
				adapter);
		final RowTransform rowTransform = adapter.getTransform();
		final IteratorConfig rowMergingCombinerConfig = new IteratorConfig(
				EnumSet.complementOf(visibilityCombinerScope),
				rowTransform.getBaseTransformPriority(),
				rowTransform.getTransformName() + ROW_MERGING_SUFFIX,
				RowMergingCombiner.class.getName(),
				optionProvider);
		final IteratorConfig rowMergingVisibilityCombinerConfig = new IteratorConfig(
				visibilityCombinerScope,
				rowTransform.getBaseTransformPriority() + 1,
				rowTransform.getTransformName() + ROW_MERGING_VISIBILITY_SUFFIX,
				RowMergingVisibilityCombiner.class.getName(),
				optionProvider);

		operations.attachIterators(
				tableName,
				createTable,
				rowMergingCombinerConfig,
				rowMergingVisibilityCombinerConfig);
	}

	private static CloseableIterator<Entry<Key, Value>> getIterator(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		CloseableIterator<Entry<Key, Value>> iterator = null;
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		if (indexStore.indexExists(index.getId())) {
			final ScannerBase scanner = operations.createBatchScanner(index.getId().getString());
			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(null));
			final IteratorSetting iteratorSettings = new IteratorSetting(
					10,
					"GEOWAVE_WHOLE_ROW_ITERATOR",
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);

			final List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
			clientFilters.add(
					0,
					new DedupeFilter());

			final Iterator<Entry<Key, Value>> it = new IteratorWrapper(
					adapterStore,
					index,
					scanner.iterator(),
					new FilterList<QueryFilter>(
							clientFilters));

			iterator = new CloseableIteratorWrapper<Entry<Key, Value>>(
					new ScannerClosableWrapper(
							scanner),
					it);
		}
		return iterator;
	}

	private static class IteratorWrapper implements
			Iterator<Entry<Key, Value>>
	{

		private final Iterator<Entry<Key, Value>> scannerIt;
		private final AdapterStore adapterStore;
		private final PrimaryIndex index;
		private final QueryFilter clientFilter;
		private Entry<Key, Value> nextValue;

		public IteratorWrapper(
				final AdapterStore adapterStore,
				final PrimaryIndex index,
				final Iterator<Entry<Key, Value>> scannerIt,
				final QueryFilter clientFilter ) {
			this.adapterStore = adapterStore;
			this.index = index;
			this.scannerIt = scannerIt;
			this.clientFilter = clientFilter;
			findNext();
		}

		private void findNext() {
			while (scannerIt.hasNext()) {
				final Entry<Key, Value> row = scannerIt.next();
				final Object decodedValue = decodeRow(
						row,
						clientFilter,
						index);
				if (decodedValue != null) {
					nextValue = row;
					return;
				}
			}
			nextValue = null;
		}

		private Object decodeRow(
				final Entry<Key, Value> row,
				final QueryFilter clientFilter,
				final PrimaryIndex index ) {
			return AccumuloUtils.decodeRow(
					row.getKey(),
					row.getValue(),
					true,
					new AccumuloRowId(
							row.getKey()), // need to pass this, otherwise null
											// value for rowId gets dereferenced
											// later
					adapterStore,
					clientFilter,
					index);
		}

		@Override
		public boolean hasNext() {
			return nextValue != null;
		}

		@Override
		public Entry<Key, Value> next() {
			final Entry<Key, Value> previousNext = nextValue;
			findNext();
			return previousNext;
		}

		@Override
		public void remove() {}
	}

}