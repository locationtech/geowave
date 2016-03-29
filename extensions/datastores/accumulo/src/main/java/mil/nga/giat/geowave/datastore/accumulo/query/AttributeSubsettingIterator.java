package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.util.BitmaskUtils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Splitter;

public class AttributeSubsettingIterator extends
		TransformingIterator
{
	private static final int ITERATOR_PRIORITY = QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY - 1;
	private static final String ITERATOR_NAME = "ATTRIBUTE_SUBSETTING_ITERATOR";

	private static final String FIELD_IDS = "fieldIds";
	private final Set<ByteArrayId> fieldIds = new HashSet<>();
	private static final String FIELD_IDS_ADAPTER = "fieldIdsAdapter";
	private DataAdapter<?> adapterAssociatedWithFieldIds;

	private static final String ADAPTERS = "adapters";
	private final List<DataAdapter> adapters = new ArrayList<>();

	private static final String MODEL = "model";
	private CommonIndexModel model;

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW_COLFAM;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {
		while (input.hasTop()) {
			final Key currKey = input.getTopKey();
			final Value currVal = input.getTopValue();
			final DataAdapter currAdapter = lookupAdapter(currKey.getColumnFamilyData().getBackingArray());
			if (currAdapter != null) {
				final boolean currAdapterIsAssociatedWithFieldIds = currAdapter.getAdapterId().equals(
						adapterAssociatedWithFieldIds.getAdapterId());
				if (currAdapterIsAssociatedWithFieldIds) {
					final byte[] compositeBitmask = currKey.getColumnQualifierData().getBackingArray();
					final List<byte[]> bitmasks = BitmaskUtils.getBitmasks(compositeBitmask);
					final List<ByteArrayId> currentFieldIds = new ArrayList<>();
					final List<ByteArrayId> fieldsToKeep = new ArrayList<>();
					final List<byte[]> bitmasksOfKeepers = new ArrayList<>();
					for (final byte[] currBitmask : bitmasks) {
						final int ordinal = BitmaskUtils.getOrdinal(currBitmask);
						final ByteArrayId fieldId = adapterAssociatedWithFieldIds.getFieldIdForPosition(
								model,
								ordinal);
						currentFieldIds.add(fieldId);
						if (fieldIds.contains(fieldId)) {
							fieldsToKeep.add(fieldId);
							bitmasksOfKeepers.add(currBitmask);
						}
					}
					if (!fieldsToKeep.isEmpty()) {
						if (fieldsToKeep.size() != currentFieldIds.size()) {
							final byte[] newBitmask = BitmaskUtils.generateCompositeBitmask(bitmasksOfKeepers);
							final Key newKey = replaceColumnQualifier(
									currKey,
									new Text(
											newBitmask));
							final Value newVal = constructNewValue(
									currVal,
									currentFieldIds,
									fieldsToKeep);
							output.append(
									newKey,
									newVal);
						}
						else {
							output.append(
									currKey,
									currVal);
						}
					}
				}
				else {
					output.append(
							currKey,
							currVal);
				}
			}
			input.next();
		}
	}

	private DataAdapter lookupAdapter(
			final byte[] adapterId ) {
		for (final DataAdapter adapter : adapters) {
			if (Arrays.equals(
					adapterId,
					adapter.getAdapterId().getBytes())) {
				return adapter;
			}
		}
		return null;
	}

	private Value constructNewValue(
			final Value original,
			final List<ByteArrayId> allFieldIds,
			final List<ByteArrayId> fieldIdsToKeep ) {
		final ByteBuffer originalBytes = ByteBuffer.wrap(original.get());
		final List<byte[]> valsToKeep = new ArrayList<>();
		int totalSize = 0;
		final int numFields = allFieldIds.size();
		for (int i = 0; i < numFields; i++) {
			final int len = originalBytes.getInt();
			final byte[] val = new byte[len];
			originalBytes.get(val);
			if (fieldIdsToKeep.contains(allFieldIds.get(i))) {
				valsToKeep.add(val);
				totalSize += len;
			}
		}
		final ByteBuffer retVal = ByteBuffer.allocate((4 * valsToKeep.size()) + totalSize);
		for (final byte[] val : valsToKeep) {
			retVal.putInt(val.length);
			retVal.put(val);
		}
		return new Value(
				retVal.array());
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		// get adapter
		final String adapterStr = options.get(ADAPTERS);
		final byte[] adapterBytes = ByteArrayUtils.byteArrayFromString(adapterStr);
		final List<Persistable> persistables = PersistenceUtils.fromBinary(adapterBytes);
		for (final Persistable persistable : persistables) {
			adapters.add((DataAdapter) persistable);
		}
		// get model
		final String modelStr = options.get(MODEL);
		final byte[] modelBytes = ByteArrayUtils.byteArrayFromString(modelStr);
		model = PersistenceUtils.fromBinary(
				modelBytes,
				CommonIndexModel.class);
		// get fieldIds and associated adapter
		final Iterable<String> fieldIdsList = Splitter.on(
				',').split(
				options.get(FIELD_IDS));
		for (final String fieldId : fieldIdsList) {
			fieldIds.add(new ByteArrayId(
					StringUtils.stringToBinary(fieldId)));
		}
		final String fieldIdsAdapterString = options.get(FIELD_IDS_ADAPTER);
		final byte[] fieldIdsAdapterBytes = ByteArrayUtils.byteArrayFromString(fieldIdsAdapterString);
		adapterAssociatedWithFieldIds = PersistenceUtils.fromBinary(
				fieldIdsAdapterBytes,
				AbstractDataAdapter.class);
	}

	@Override
	public boolean validateOptions(
			final Map<String, String> options ) {
		if ((!super.validateOptions(options)) || (options == null)) {
			return false;
		}
		final boolean hasAdapters = options.containsKey(ADAPTERS);
		final boolean hasModel = options.containsKey(MODEL);
		final boolean hasFieldIds = options.containsKey(FIELD_IDS);
		final boolean hasAdapterForFieldIds = options.containsKey(FIELD_IDS_ADAPTER);
		if (!hasAdapters || !hasModel || !hasFieldIds || !hasAdapterForFieldIds) {
			// all are required
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @return an {@link IteratorSetting} for this iterator
	 */
	public static IteratorSetting getIteratorSetting() {
		return new IteratorSetting(
				AttributeSubsettingIterator.ITERATOR_PRIORITY,
				AttributeSubsettingIterator.ITERATOR_NAME,
				AttributeSubsettingIterator.class);
	}

	/**
	 * Sets the desired subset of fields to keep
	 * 
	 * @param setting
	 *            the {@link IteratorSetting}
	 * @param adapterAssociatedWithFieldIds
	 *            the adapter associated with the given fieldIds
	 * @param fieldIds
	 *            the desired subset of fieldIds
	 * @param numericDimensions
	 *            the numeric dimension fields
	 */
	public static void setFieldIds(
			final IteratorSetting setting,
			final DataAdapter<?> adapterAssociatedWithFieldIds,
			final List<String> fieldIds,
			final NumericDimensionField<? extends CommonIndexValue>[] numericDimensions ) {
		final Set<String> desiredSubset = new TreeSet<>();
		// add fieldIds
		desiredSubset.addAll(fieldIds);
		// dimension fields must also be included
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : numericDimensions) {
			desiredSubset.add(StringUtils.stringFromBinary(dimension.getFieldId().getBytes()));
		}
		final String fieldIdsString = org.apache.commons.lang3.StringUtils.join(
				desiredSubset,
				',');
		setting.addOption(
				FIELD_IDS,
				fieldIdsString);
		final String adapterString = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapterAssociatedWithFieldIds));
		setting.addOption(
				FIELD_IDS_ADAPTER,
				adapterString);
	}

	/**
	 * Sets the Data Adapters for use by this iterator
	 * 
	 * @param setting
	 *            the {@link IteratorSetting}
	 * @param adapter
	 *            the {@link DataAdapter}
	 */
	public static void setAdapters(
			final IteratorSetting setting,
			final List<DataAdapter> adapters ) {
		final List<Persistable> persistables = new ArrayList<>();
		for (final DataAdapter adapter : adapters) {
			persistables.add(adapter);
		}
		final String adapterString = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(persistables));
		setting.addOption(
				ADAPTERS,
				adapterString);
	}

	/**
	 * Sets the {@link CommonIndexModel} for use by this iterator
	 * 
	 * @param setting
	 *            the {@link IteratorSetting}
	 * @param model
	 *            the {@link CommonIndexModel}
	 */
	public static void setModel(
			final IteratorSetting setting,
			final CommonIndexModel model ) {
		final String modelString = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(model));
		setting.addOption(
				MODEL,
				modelString);
	}

}
