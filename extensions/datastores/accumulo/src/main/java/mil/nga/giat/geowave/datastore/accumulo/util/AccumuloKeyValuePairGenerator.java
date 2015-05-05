package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * 
 * Given a {@link WritableDataAdapter} and an {@link Index}, this class handles
 * the creation of Geowave-formatted [Key,Value] pairs.
 * 
 * The intent is that this class will be used within the Mapper of a MapReduce
 * job to generate Keys and Values to be sorted during the shuffle-and-sort
 * phase in preparation for bulk ingest into Accumulo via
 * {@link AccumuloFileOutputFormat}.
 * 
 * @param <T>
 *            the type of entries to index within Geowave
 */
public class AccumuloKeyValuePairGenerator<T>
{

	private WritableDataAdapter<T> adapter;
	private Index index;
	private VisibilityWriter<T> visibilityWriter;

	public AccumuloKeyValuePairGenerator(
			WritableDataAdapter<T> adapter,
			Index index,
			VisibilityWriter<T> visibilityWriter ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.visibilityWriter = visibilityWriter;
	}

	public List<AccumuloKeyValuePair> constructKeyValuePairs(
			byte[] adapterId,
			T entry ) {

		List<AccumuloKeyValuePair> keyValuePairs = new ArrayList<>();
		Key key;
		Value value;
		AccumuloKeyValuePair keyValuePair;
		DataStoreEntryInfo ingestInfo = AccumuloUtils.getIngestInfo(
				adapter,
				index,
				entry,
				visibilityWriter);
		List<ByteArrayId> rowIds = ingestInfo.getRowIds();
		@SuppressWarnings("rawtypes")
		List<FieldInfo> fieldInfoList = ingestInfo.getFieldInfo();

		for (ByteArrayId rowId : rowIds) {
			for (@SuppressWarnings("rawtypes")
			FieldInfo fieldInfo : fieldInfoList) {
				key = new Key(
						rowId.getBytes(),
						adapterId,
						fieldInfo.getDataValue().getId().getBytes(),
						fieldInfo.getVisibility(),
						System.currentTimeMillis());
				value = new Value(
						fieldInfo.getWrittenValue());
				keyValuePair = new AccumuloKeyValuePair(
						key,
						value);
				keyValuePairs.add(keyValuePair);
			}
		}

		return keyValuePairs;
	}

}
