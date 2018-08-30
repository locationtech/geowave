package mil.nga.giat.geowave.datastore.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;

public class HBaseRow implements
		GeoWaveRow
{
	private final GeoWaveKey key;
	private final GeoWaveValue[] fieldValues;

	public HBaseRow(
			final Result result,
			final int partitionKeyLength ) {
		// TODO: GEOWAVE-1018 - can we do something more clever that lazily
		// parses only whats required by the getter (and caches anything else
		// that is parsed)?
		key = new GeoWaveKeyImpl(
				result.getRow(),
				partitionKeyLength);

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMapping = result.getMap();
		List<GeoWaveValue> fieldValueList = new ArrayList();

		for (final Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cfEntry : rowMapping.entrySet()) {
			for (final Entry<byte[], NavigableMap<Long, byte[]>> cqEntry : cfEntry.getValue().entrySet()) {
				for (Entry<Long, byte[]> cqEntryValue : cqEntry.getValue().entrySet()) {
					byte[] byteValue = cqEntryValue.getValue();
					byte[] qualifier = cqEntry.getKey();

					fieldValueList.add(new GeoWaveValueImpl(
							qualifier,
							null,
							byteValue));
				}
			}
		}

		fieldValues = new GeoWaveValue[fieldValueList.size()];
		int i = 0;

		for (GeoWaveValue gwValue : fieldValueList) {
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
