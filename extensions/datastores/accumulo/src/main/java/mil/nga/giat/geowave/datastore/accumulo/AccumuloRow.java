package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Map;
import java.util.Map.Entry;

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
	// private final Map<Key, Value> rowMapping;
	private final GeoWaveValue[] fieldValues;

	public AccumuloRow(
			final byte[] rowBytes,
			final int partitionKeyLength,
			final Map<Key, Value> rowMapping ) {
		// TODO: GEOWAVE-1018 - can we do something more clever that lazily
		// parses only whats required by the getter (and caches anything else
		// that is parsed)?
		key = new GeoWaveKeyImpl(
				rowBytes,
				partitionKeyLength);
		fieldValues = new GeoWaveValue[rowMapping.size()];
		int i = 0;
		for (final Entry<Key, Value> kv : rowMapping.entrySet()) {
			fieldValues[i++] = new GeoWaveValueImpl(
					kv.getKey().getColumnQualifier().getBytes(),
					kv.getKey().getColumnVisibility().getBytes(),
					kv.getValue().get());
		}
	}

	@Override
	public byte[] getDataId() {
		return key.getDataId();
	}

	@Override
	public byte[] getAdapterId() {
		return key.getAdapterId();
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
