package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;

public class AccumuloRow implements
		GeoWaveRow
{
	private final GeoWaveKey key;
	private GeoWaveValue[] fieldValues;

	private static class LatestFirstComparator implements
			Comparator<Long>,
			Serializable
	{
		public int compare(
				Long ts1,
				Long ts2 ) {
			return ts2.compareTo(ts1);
		}
	}

	public AccumuloRow(
			final byte[] rowBytes,
			final int partitionKeyLength,
			final List<Map<Key, Value>> fieldValueMapList,
			final boolean sortByTime ) {
		// TODO: GEOWAVE-1018 - can we do something more clever that lazily
		// parses only whats required by the getter (and caches anything else
		// that is parsed)?
		key = new GeoWaveKeyImpl(
				rowBytes,
				partitionKeyLength);

		if (sortByTime) {
			setTimeSortedFieldValues(fieldValueMapList);
		}
		else {
			setFieldValues(fieldValueMapList);
		}
	}

	private void setFieldValues(
			List<Map<Key, Value>> fieldValueMapList ) {
		List<GeoWaveValue> fieldValueList = new ArrayList();

		for (final Map<Key, Value> kvMap : fieldValueMapList) {
			for (final Entry<Key, Value> kv : kvMap.entrySet()) {
				fieldValueList.add(new GeoWaveValueImpl(
						kv.getKey().getColumnQualifier().getBytes(),
						kv.getKey().getColumnVisibility().getBytes(),
						kv.getValue().get()));
			}
		}

		fieldValues = new GeoWaveValue[fieldValueList.size()];
		int i = 0;

		for (GeoWaveValue gwValue : fieldValueList) {
			fieldValues[i++] = gwValue;
		}
	}

	private void setTimeSortedFieldValues(
			final List<Map<Key, Value>> fieldValueMapList ) {
		SortedMap<Long, GeoWaveValue> fieldValueSortedMap = new TreeMap(
				new LatestFirstComparator());

		for (final Map<Key, Value> kvMap : fieldValueMapList) {
			for (final Entry<Key, Value> kv : kvMap.entrySet()) {
				fieldValueSortedMap.put(
						kv.getKey().getTimestamp(),
						new GeoWaveValueImpl(
								kv.getKey().getColumnQualifier().getBytes(),
								kv.getKey().getColumnVisibility().getBytes(),
								kv.getValue().get()));
			}
		}

		Iterator it = fieldValueSortedMap.entrySet().iterator();

		fieldValues = new GeoWaveValue[fieldValueSortedMap.size()];
		int i = 0;

		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			GeoWaveValue gwValue = (GeoWaveValue) entry.getValue();
			fieldValues[i++] = gwValue;
		}
	}

	@Override
	public byte[] getDataId() {
		return key.getDataId();
	}

	@Override
	public short getInternalAdapterId() {
		return key.getInternalAdapterId();
	}

	@Override
	public byte[] getSortKey() {
		return key.getSortKey();
	}

	@Override
	public byte[] getPartitionKey() {
		return key.getPartitionKey();
	}

	@Override
	public int getNumberOfDuplicates() {
		return key.getNumberOfDuplicates();
	}

	@Override
	public GeoWaveValue[] getFieldValues() {
		return fieldValues;
	}
}
