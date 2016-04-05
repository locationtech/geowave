package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.base.Splitter;

public class FieldFilter extends
		Filter
{
	// must execute prior to WholeRowIterator/QueryFilterIterator but after
	// SharedVisibilitySplittingIterator
	private static final int ITERATOR_PRIORITY = QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY - 1;
	private static final String ITERATOR_NAME = "FIELD_FILTER_ITERATOR";
	private static final String FIELD_IDS = "fieldIds";
	private final Set<ByteArrayId> fieldIds = new HashSet<>();

	@Override
	public boolean validateOptions(
			final Map<String, String> options ) {
		if ((!super.validateOptions(options)) || (options == null) || (!options.containsKey(FIELD_IDS))) {
			return false;
		}
		return true;
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
		final Iterable<String> fieldIdsList = Splitter.on(
				',').split(
				options.get(FIELD_IDS));
		for (final String fieldId : fieldIdsList) {
			fieldIds.add(new ByteArrayId(
					StringUtils.stringToBinary(fieldId)));
		}
	}

	@Override
	public boolean accept(
			final Key k,
			final Value v ) {
		for (final ByteArrayId fieldId : fieldIds) {
			if (Arrays.equals(
					k.getColumnQualifierData().getBackingArray(),
					fieldId.getBytes())) {
				return true;
			}
		}
		return false;
	}

	public static IteratorSetting getIteratorSetting() {
		return new IteratorSetting(
				FieldFilter.ITERATOR_PRIORITY,
				FieldFilter.ITERATOR_NAME,
				FieldFilter.class);
	}

	public static void setFieldIds(
			final IteratorSetting setting,
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
				FieldFilter.FIELD_IDS,
				fieldIdsString);
	}

}
