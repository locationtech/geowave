package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.flatten.BitmaskedPairComparator;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class BaseDataStoreUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseDataStoreUtils.class);
	public static final int MAX_RANGE_DECOMPOSITION = 2000;
	public static final int AGGREGATION_RANGE_DECOMPOSITION = 10;

	public static <T> GeoWaveRow[] getGeoWaveRows(
			final T entry,
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		return getWriteInfo(
				entry,
				adapter,
				index,
				customFieldVisibilityWriter).getRows();
	}

	/**
	 * Basic method that decodes a native row Currently overridden by Accumulo
	 * and HBase; Unification in progress
	 *
	 * Override this method if you can't pass in a GeoWaveRow!
	 */
	public static <T> Object decodeRow(
			final GeoWaveRow geowaveRow,
			final QueryFilter clientFilter,
			final DataAdapter<T> adapter,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final ScanCallback scanCallback,
			final byte[] fieldSubsetBitmask,
			final boolean decodeRow ) {
		final ByteArrayId adapterId = new ByteArrayId(
				geowaveRow.getAdapterId());

		if ((adapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		final IntermediaryReadEntryInfo decodePackage = new IntermediaryReadEntryInfo(
				index,
				decodeRow);

		if (!decodePackage.setOrRetrieveAdapter(
				adapter,
				adapterId,
				adapterStore)) {
			LOGGER.error("Could not retrieve adapter from adapter store.");
			return null;
		}

		// Verify the adapter matches the data
		if (!decodePackage.isAdapterVerified()) {
			if (!decodePackage.verifyAdapter(adapterId)) {
				LOGGER.error("Adapter verify failed: adapter does not match data.");
				return null;
			}
		}

		for (final GeoWaveValue value : geowaveRow.getFieldValues()) {
			byte[] byteValue = value.getValue();
			byte[] fieldMask = value.getFieldMask();
			if (fieldSubsetBitmask != null) {
				final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
						fieldMask,
						fieldSubsetBitmask);
				byteValue = BitmaskUtils.constructNewValue(
						byteValue,
						fieldMask,
						newBitmask);
				if ((byteValue == null) || (byteValue.length == 0)) {
					continue;
				}
				fieldMask = newBitmask;
			}

			readValue(
					decodePackage,
					new GeoWaveValueImpl(
							fieldMask,
							value.getVisibility(),
							byteValue));
		}

		return getDecodedRow(
				geowaveRow,
				decodePackage,
				clientFilter,
				scanCallback);
	}

	public static CloseableIterator<Object> aggregate(
			final CloseableIterator<Object> it,
			final Aggregation aggregationFunction ) {
		if ((it != null) && it.hasNext()) {
			synchronized (aggregationFunction) {
				aggregationFunction.clearResult();
				while (it.hasNext()) {
					final Object input = it.next();
					if (input != null) {
						aggregationFunction.aggregate(input);
					}
				}
				try {
					it.close();
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to close datastore reader",
							e);
				}

				return new Wrapper(
						Iterators.singletonIterator(aggregationFunction.getResult()));
			}
		}
		return new CloseableIterator.Empty();
	}

	/**
	 * build a persistence encoding object first, pass it through the client
	 * filters and if its accepted, use the data adapter to decode the
	 * persistence model into the native data type
	 */
	private static <T> Object getDecodedRow(
			final GeoWaveRow row,
			final IntermediaryReadEntryInfo<T> decodePackage,
			final QueryFilter clientFilter,
			final ScanCallback<T, GeoWaveRow> scanCallback ) {
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				decodePackage.getDataAdapter().getAdapterId(),
				new ByteArrayId(
						row.getDataId()),
				new ByteArrayId(
						row.getPartitionKey()),
				new ByteArrayId(
						row.getSortKey()),
				row.getNumberOfDuplicates(),
				decodePackage.getIndexData(),
				decodePackage.getUnknownData(),
				decodePackage.getExtendedData());

		if ((clientFilter == null) || clientFilter.accept(
				decodePackage.getIndex().getIndexModel(),
				encodedRow)) {
			if (!decodePackage.isDecodeRow()) {
				return encodedRow;
			}

			final T decodedRow = decodePackage.getDataAdapter().decode(
					encodedRow,
					decodePackage.getIndex());

			if (scanCallback != null) {
				scanCallback.entryScanned(
						decodedRow,
						row);
			}

			return decodedRow;
		}

		return null;
	}

	/**
	 * Generic field reader - updates fieldInfoList from field input data
	 */
	private static void readValue(
			final IntermediaryReadEntryInfo decodePackage,
			final GeoWaveValue value ) {
		final List<FlattenedFieldInfo> fieldInfos = DataStoreUtils.decomposeFlattenedFields(
				value.getFieldMask(),
				value.getValue(),
				value.getVisibility(),
				-1).getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final ByteArrayId fieldId = decodePackage.getDataAdapter().getFieldIdForPosition(
					decodePackage.getIndex().getIndexModel(),
					fieldInfo.getFieldPosition());
			final FieldReader<? extends CommonIndexValue> indexFieldReader = decodePackage
					.getIndex()
					.getIndexModel()
					.getReader(
							fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldInfo.getValue());
				indexValue.setVisibility(value.getVisibility());
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				decodePackage.getIndexData().addValue(
						val);
			}
			else {
				final FieldReader<?> extFieldReader = decodePackage.getDataAdapter().getReader(
						fieldId);
				if (extFieldReader != null) {
					final Object objValue = extFieldReader.readField(fieldInfo.getValue());
					final PersistentValue<Object> val = new PersistentValue<Object>(
							fieldId,
							objValue);
					// TODO GEOWAVE-1018, do we care about visibility
					decodePackage.getExtendedData().addValue(
							val);
				}
				else {
					LOGGER.error("field reader not found for data entry, the value may be ignored");
					decodePackage.getUnknownData().addValue(
							new PersistentValue<byte[]>(
									fieldId,
									fieldInfo.getValue()));
				}
			}
		}
	}

	protected static <T> IntermediaryWriteEntryInfo getWriteInfo(
			final T entry,
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel = index.getIndexModel();

		final AdapterPersistenceEncoding encodedData = adapter.encode(
				entry,
				indexModel);
		final InsertionIds insertionIds = encodedData.getInsertionIds(index);
		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		final byte[] dataId = adapter.getDataId(
				entry).getBytes();
		final byte[] adapterId = adapter.getAdapterId().getBytes();
		if (!insertionIds.isEmpty()) {
			for (final PersistentValue fieldValue : commonValues) {
				final FieldInfo<?> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue,
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(fieldInfo);
				}
			}
			for (final PersistentValue<?> fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<?> fieldInfo = getFieldInfo(
							adapter,
							fieldValue,
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(fieldInfo);
					}
				}
			}
		}
		else {
			LOGGER.warn("Indexing failed to produce insertion ids; entry [" + adapter.getDataId(
					entry).getString() + "] not saved.");
		}

		fieldInfoList = BaseDataStoreUtils.composeFlattenedFields(
				fieldInfoList,
				index.getIndexModel(),
				adapter);
		// TODO GEOWAVE-1018 need to figure out the correct way to do this for
		// all data stores
		byte[] uniqueDataId;
		// if ((adapter instanceof RowMergingDataAdapter) &&
		// (((RowMergingDataAdapter) adapter).getTransform() != null)) {
		// uniqueDataId = DataStoreUtils.ensureUniqueId(
		// dataId,
		// false).getBytes();
		// }
		// else {
		uniqueDataId = dataId;
		// }

		return new IntermediaryWriteEntryInfo(
				uniqueDataId,
				adapterId,
				insertionIds,
				fieldInfoList);
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
				listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(
						fieldPosition,
						fieldInfo));
			}
			else {
				final List<Pair<Integer, FieldInfo<?>>> listForViz = new ArrayList<>();
				listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(
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
					new BitmaskedPairComparator());
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

	private static <T> FieldInfo<?> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<?> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter
				.getFieldVisibilityHandler(fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			return new FieldInfo(
					fieldValue,
					fieldWriter.writeField(value),
					DataStoreUtils.mergeVisibilities(
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
}
