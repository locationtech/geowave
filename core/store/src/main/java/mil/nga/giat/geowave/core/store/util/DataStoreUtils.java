package mil.nga.giat.geowave.core.store.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayRange.MergeOperation;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.flatten.BitmaskedFieldInfoComparator;
import mil.nga.giat.geowave.core.store.flatten.FlattenedDataSet;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadDataSingleRow;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/*
 */
public class DataStoreUtils
{
	private final static Logger LOGGER = Logger.getLogger(DataStoreUtils.class);

	// we append a 0 byte, 8 bytes of timestamp, and 16 bytes of UUID
	public final static int UNIQUE_ADDED_BYTES = 1 + 8 + 16;
	public final static byte UNIQUE_ID_DELIMITER = 0;
	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final UniformVisibilityWriter UNCONSTRAINED_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	public static final byte[] EMTPY_VISIBILITY = new byte[] {};

	public static List<ByteArrayId> getUniqueDimensionFields(
			final CommonIndexModel model ) {
		final List<ByteArrayId> dimensionFieldIds = new ArrayList<>();
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : model.getDimensions()) {
			if (!dimensionFieldIds.contains(dimension.getFieldId())) {
				dimensionFieldIds.add(dimension.getFieldId());
			}
		}
		return dimensionFieldIds;
	}

	public static <T> long cardinality(
			final PrimaryIndex index,
			final Map<ByteArrayId, DataStatistics<T>> stats,
			final List<ByteArrayRange> ranges ) {
		final RowRangeHistogramStatistics rangeStats = (RowRangeHistogramStatistics) stats
				.get(RowRangeHistogramStatistics.composeId(index.getId()));
		if (rangeStats == null) {
			return Long.MAX_VALUE - 1;
		}
		long count = 0;
		for (final ByteArrayRange range : ranges) {
			count += rangeStats.cardinality(
					range.getStart().getBytes(),
					range.getEnd().getBytes());
		}
		return count;
	}

	public static boolean rowIdsMatch(
			final GeoWaveRow rowId1,
			final GeoWaveRow rowId2 ) {
		if (!Arrays.equals(
				rowId1.getIndex(),
				rowId2.getIndex())) {
			return false;
		}
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
				removeUniqueId(rowId1.getDataId()),
				removeUniqueId(rowId2.getDataId()));
	}

	public static byte[] removeUniqueId(
			byte[] dataId ) {
		if ((dataId.length < UNIQUE_ADDED_BYTES) || (dataId[dataId.length - UNIQUE_ADDED_BYTES] != UNIQUE_ID_DELIMITER)) {
			return dataId;
		}

		dataId = Arrays.copyOfRange(
				dataId,
				0,
				dataId.length - UNIQUE_ADDED_BYTES);

		return dataId;
	}

	public static <T> void readFieldInfo(
			final List<FieldInfo<?>> fieldInfoList,
			final PersistentDataset<CommonIndexValue> indexData,
			final PersistentDataset<Object> extendedData,
			final PersistentDataset<byte[]> unknownData,
			final byte[] compositeFieldIdBytes,
			final byte[] commonVisiblity,
			final byte[] byteValue,
			final DataAdapter<T> adapter,
			final CommonIndexModel indexModel ) {
		final ByteArrayId compositeFieldId = new ByteArrayId(
				compositeFieldIdBytes);
		final List<FlattenedFieldInfo> fieldInfos = DataStoreUtils.decomposeFlattenedFields(
				compositeFieldId.getBytes(),
				byteValue,
				commonVisiblity,
				-1).getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final ByteArrayId fieldId = adapter.getFieldIdForPosition(
					indexModel,
					fieldInfo.getFieldPosition());
			final FieldReader<? extends CommonIndexValue> indexFieldReader = indexModel.getReader(fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldInfo.getValue());
				indexValue.setVisibility(commonVisiblity);
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				indexData.addValue(val);
				fieldInfoList.add(DataStoreUtils.getFieldInfo(
						val,
						fieldInfo.getValue(),
						commonVisiblity));
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
							commonVisiblity));
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
	public static <T> FlattenedDataSet decomposeFlattenedFields(
			final byte[] bitmask,
			final byte[] flattenedValue,
			final byte[] commonVisibility,
			final int maxFieldPosition ) {
		final List<FlattenedFieldInfo> fieldInfoList = new ArrayList<FlattenedFieldInfo>();
		final List<Integer> fieldPositions = BitmaskUtils.getFieldPositions(bitmask);

		final boolean sharedVisibility = fieldPositions.size() > 1;
		if (sharedVisibility) {
			final ByteBuffer input = ByteBuffer.wrap(flattenedValue);
			for (int i = 0; i < fieldPositions.size(); i++) {
				final Integer fieldPosition = fieldPositions.get(i);
				if ((maxFieldPosition > -1) && (fieldPosition > maxFieldPosition)) {
					return new FlattenedDataSet(
							fieldInfoList,
							new FlattenedUnreadDataSingleRow(
									input,
									i,
									fieldPositions));
				}
				final int fieldLength = input.getInt();
				final byte[] fieldValueBytes = new byte[fieldLength];
				input.get(fieldValueBytes);
				fieldInfoList.add(new FlattenedFieldInfo(
						fieldPosition,
						fieldValueBytes));
			}
		}
		else {
			fieldInfoList.add(new FlattenedFieldInfo(
					fieldPositions.get(0),
					flattenedValue));

		}
		return new FlattenedDataSet(
				fieldInfoList,
				null);
	}

	public static List<ByteArrayRange> constraintsToByteArrayRanges(
			final List<MultiDimensionalNumericData> constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges,
			final IndexMetaData... hints ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
			// positive infinity
		}
		else {
			final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
			for (final MultiDimensionalNumericData nd : constraints) {
				ranges.addAll(indexStrategy.getQueryRanges(
						nd,
						maxRanges,
						hints));
			}
			if (constraints.size() > 1) {
				return ByteArrayRange.mergeIntersections(
						ranges,
						MergeOperation.UNION);
			}
			return ranges;
		}
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_"
				+ unqualifiedTableName;
	}

	public static List<IndexedAdapterPersistenceEncoding> getEncodings(
			final PrimaryIndex index,
			final AdapterPersistenceEncoding encoding ) {
		final List<ByteArrayId> ids = encoding.getInsertionIds(index);
		final ArrayList<IndexedAdapterPersistenceEncoding> encodings = new ArrayList<IndexedAdapterPersistenceEncoding>();
		for (final ByteArrayId id : ids) {
			encodings.add(new IndexedAdapterPersistenceEncoding(
					encoding.getAdapterId(),
					encoding.getDataId(),
					id,
					ids.size(),
					encoding.getCommonData(),
					encoding.getUnknownData(),
					encoding.getAdapterExtendedData()));
		}
		return encodings;
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

		addToRowIds(
				rowIds,
				insertionIds,
				dataWriter.getDataId(
						entry).getBytes(),
				dataWriter.getAdapterId().getBytes(),
				encodedData.isDeduplicationEnabled());

		return rowIds;
	}

	public static <T> void addToRowIds(
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
					new GeoWaveRowImpl(
							dataId,
							adapterId,
							indexId,
							enableDeduplication ? numberOfDuplicates : -1).getRowId()));
		}
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> DataStoreEntryInfo getIngestInfo(
			final WritableDataAdapter<T> dataWriter,
			final PrimaryIndex index,
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

		final List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		final byte[] dataId = dataWriter.getDataId(
				entry).getBytes();
		if (!insertionIds.isEmpty()) {
			addToRowIds(
					rowIds,
					insertionIds,
					dataId,
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
			return new DataStoreEntryInfo(
					dataId,
					insertionIds,
					rowIds,
					fieldInfoList);
		}
		LOGGER.warn("Indexing failed to produce insertion ids; entry [" + dataWriter.getDataId(
				entry).getString() + "] not saved.");
		return new DataStoreEntryInfo(
				dataId,
				Collections.EMPTY_LIST,
				Collections.EMPTY_LIST,
				Collections.EMPTY_LIST);

	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> FieldInfo<T> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<T> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter
				.getFieldVisibilityHandler(fieldValue.getId());
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
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for "
					+ fieldValue.getValue());
		}
		return null;
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> FieldInfo<T> getFieldInfo(
			final PersistentValue<T> fieldValue,
			final byte[] value,
			final byte[] visibility ) {
		return new FieldInfo<T>(
				fieldValue,
				value,
				visibility);
	}

	private static final byte[] BEG_AND_BYTE = "&".getBytes(StringUtils.GEOWAVE_CHAR_SET);
	private static final byte[] END_AND_BYTE = ")".getBytes(StringUtils.GEOWAVE_CHAR_SET);

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

	/**
	 * This method combines all FieldInfos that share a common visibility into a
	 * single FieldInfo
	 *
	 * @param originalList
	 * @return a new list of composite FieldInfos
	 */
	public static <T> List<FieldInfo<?>> composeFlattenedFields(
			final List<FieldInfo<?>> originalList,
			final CommonIndexModel model,
			final WritableDataAdapter<?> writableAdapter ) {
		final List<FieldInfo<?>> retVal = new ArrayList<>();
		final Map<ByteArrayId, List<Pair<Integer, FieldInfo<?>>>> vizToFieldMap = new LinkedHashMap<>();
		boolean sharedVisibility = false;
		// organize FieldInfos by unique visibility
		for (final FieldInfo<?> fieldInfo : originalList) {
			int fieldPosition = writableAdapter.getPositionOfOrderedField(
					model,
					fieldInfo.getDataValue().getId());
			if (fieldPosition == -1) {
				// this is just a fallback for unexpected failures
				fieldPosition = writableAdapter.getPositionOfOrderedField(
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

	@SuppressWarnings("unchecked")
	public static <T, R extends GeoWaveRow> T decodeRow(
			final R row,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T, R> scanCallback ) {
		return (T) decodeRowInternal(
				row,
				null,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

	@SuppressWarnings("unchecked")
	private static <T, R extends GeoWaveRow> T decodeRowInternal(
			final R row,
			DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T, R> scanCallback ) {
		if (dataAdapter == null) {
			if (adapterStore != null) {
				ByteArrayId adapterId = new ByteArrayId(
						row.getAdapterId());
				dataAdapter = (DataAdapter<T>) adapterStore.getAdapter(adapterId);
			}
			if (dataAdapter == null) {
				LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
				return null;
			}
		}

		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();

		final List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		final List<FlattenedFieldInfo> flattenedFieldInfoList = new ArrayList<FlattenedFieldInfo>();
		final CommonIndexModel indexModel = index.getIndexModel();
		final byte[] flattenedValue = row.getValue();
		final ByteBuffer input = ByteBuffer.wrap(flattenedValue);

		// this in particular is a terrible hack to find raster data
		if (dataAdapter.getFieldIdForPosition(
				indexModel,
				indexModel.getDimensions().length).equals(
				new ByteArrayId(
						"image"))) {
			final ByteArrayId fieldId = new ByteArrayId(
					"image");
			final FieldReader<?> reader = dataAdapter.getReader(fieldId);
			final byte[] bytes = input.array();
			final PersistentValue<Object> val = new PersistentValue<Object>(
					fieldId,
					reader.readField(bytes));
			extendedData.addValue(val);
			fieldInfoList.add(DataStoreUtils.getFieldInfo(
					val,
					bytes,
					new byte[] {}));
		}
		else {
			// Get the list of valid field positions from the row's field mask
			List<Integer> fieldPositions = BitmaskUtils.getFieldPositions(row.getFieldMask());

			// Collect the valid fields
			int fieldIndex = 0;
			while (input.hasRemaining()) {
				final int fieldLength = input.getInt();

				final byte[] fieldValueBytes = new byte[fieldLength];
				input.get(fieldValueBytes);

				if (fieldPositions.contains(fieldIndex)) {
					flattenedFieldInfoList.add(new FlattenedFieldInfo(
							fieldIndex,
							fieldValueBytes));
				}

				fieldIndex++;
			}

			// below this likely needs some work
			final Set<ByteArrayId> visitedFieldIds = new HashSet<>();
			for (final FlattenedFieldInfo flatInfo : flattenedFieldInfoList) {
				final ByteArrayId fieldId = dataAdapter.getFieldIdForPosition(
						indexModel,
						flatInfo.getFieldPosition());
				if (!visitedFieldIds.contains(fieldId)) {
					visitedFieldIds.add(fieldId);
					final FieldReader<? extends CommonIndexValue> indexFieldReader = indexModel.getReader(fieldId);
					if (indexFieldReader != null) {
						final CommonIndexValue indexValue = indexFieldReader.readField(flatInfo.getValue());
						final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
								fieldId,
								indexValue);
						indexData.addValue(val);
						fieldInfoList.add(DataStoreUtils.getFieldInfo(
								val,
								flatInfo.getValue(),
								new byte[] {}));
					}
					else {
						final FieldReader<?> extFieldReader = dataAdapter.getReader(fieldId);
						if (extFieldReader != null) {
							final Object value = extFieldReader.readField(flatInfo.getValue());
							final PersistentValue<Object> val = new PersistentValue<Object>(
									fieldId,
									value);
							extendedData.addValue(val);
							fieldInfoList.add(DataStoreUtils.getFieldInfo(
									val,
									flatInfo.getValue(),
									new byte[] {}));
						}
						else {
							LOGGER.error("field reader not found for data entry, the value may be ignored");
							unknownData.addValue(new PersistentValue<byte[]>(
									fieldId,
									flatInfo.getValue()));
						}
					}
				}
			}
		}
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				dataAdapter.getAdapterId(),
				new ByteArrayId(
						row.getDataId()),
				new ByteArrayId(
						row.getIndex()),
				row.getNumberOfDuplicates(),
				indexData,
				unknownData,
				extendedData);

		if ((clientFilter == null) || clientFilter.accept(
				index.getIndexModel(),
				encodedRow)) {
			final Pair<T, DataStoreEntryInfo> pair = Pair.of(
					dataAdapter.decode(
							encodedRow,
							index),
					new DataStoreEntryInfo(
							row.getDataId(),
							Arrays.asList(new ByteArrayId(
									row.getIndex())),
							Arrays.asList(new ByteArrayId(
									row.getIndex())),
							fieldInfoList));
			if (scanCallback != null) {
				scanCallback.entryScanned(
						pair.getRight(),
						row,
						pair.getLeft());
			}
			return pair.getLeft();
		}
		return null;
	}

	// TODO: this doesn't account for varying visibilities, so this is
	// potentially only temporary
	public static ByteBuffer serializeFields(
			DataStoreEntryInfo ingestInfo ) {
		final List<byte[]> fieldInfoBytesList = new ArrayList<>();
		int totalLength = 0;
		// TODO potentially another hack, but if there is only one field, don't
		// need to write the length
		if (ingestInfo.getFieldInfo().size() == 1) {
			byte[] value = ingestInfo.getFieldInfo().get(
					0).getWrittenValue();
			fieldInfoBytesList.add(value);
			totalLength += value.length;
		}
		else {
			for (final FieldInfo<?> fieldInfo : ingestInfo.getFieldInfo()) {
				final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(4 + fieldInfo.getWrittenValue().length);
				fieldInfoBytes.putInt(fieldInfo.getWrittenValue().length);
				fieldInfoBytes.put(fieldInfo.getWrittenValue());
				fieldInfoBytesList.add(fieldInfoBytes.array());
				totalLength += fieldInfoBytes.array().length;
			}
		}
		final ByteBuffer allFields = ByteBuffer.allocate(totalLength);
		for (final byte[] bytes : fieldInfoBytesList) {
			allFields.put(bytes);
		}
		return allFields;
	}

	public static ByteArrayId ensureUniqueId(
			final byte[] id,
			final boolean hasMetadata ) {

		final ByteBuffer buf = ByteBuffer.allocate(id.length + UNIQUE_ADDED_BYTES);

		byte[] metadata = null;
		byte[] dataId;
		if (hasMetadata) {
			int metadataStartIdx = id.length - 12;
			byte[] lengths = Arrays.copyOfRange(
					id,
					metadataStartIdx,
					id.length);

			final ByteBuffer lengthsBuf = ByteBuffer.wrap(lengths);
			final int adapterIdLength = lengthsBuf.getInt();
			int dataIdLength = lengthsBuf.getInt();
			dataIdLength += UNIQUE_ADDED_BYTES;
			final int duplicates = lengthsBuf.getInt();

			final ByteBuffer newLengths = ByteBuffer.allocate(12);
			newLengths.putInt(adapterIdLength);
			newLengths.putInt(dataIdLength);
			newLengths.putInt(duplicates);
			newLengths.rewind();
			metadata = newLengths.array();
			dataId = Arrays.copyOfRange(
					id,
					0,
					metadataStartIdx);
		}
		else {
			dataId = id;
		}

		buf.put(dataId);

		final long timestamp = System.currentTimeMillis();
		buf.put(new byte[] {
			UNIQUE_ID_DELIMITER
		});
		UUID uuid = UUID.randomUUID();
		buf.putLong(timestamp);
		buf.putLong(uuid.getLeastSignificantBits());
		buf.putLong(uuid.getMostSignificantBits());
		if (hasMetadata) {
			buf.put(metadata);
		}

		return new ByteArrayId(
				buf.array());
	}
}
