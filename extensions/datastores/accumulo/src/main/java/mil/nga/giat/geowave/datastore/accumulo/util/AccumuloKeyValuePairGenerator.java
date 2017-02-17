package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

/**
 * 
 * Given a {@link WritableDataAdapter} and an {@link PrimaryIndex}, this class
 * handles the creation of Geowave-formatted [Key,Value] pairs.
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
	private PrimaryIndex index;
	private VisibilityWriter<T> visibilityWriter;
	private IngestCallback<T> callback;

	public AccumuloKeyValuePairGenerator(
			WritableDataAdapter<T> adapter,
			PrimaryIndex index,
			VisibilityWriter<T> visibilityWriter ) {
		this(
				adapter,
				index,
				null,
				visibilityWriter);
	}

	public AccumuloKeyValuePairGenerator(
			WritableDataAdapter<T> adapter,
			PrimaryIndex index,
			IngestCallback<T> callback,
			VisibilityWriter<T> visibilityWriter ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.callback = callback;
		this.visibilityWriter = visibilityWriter;
	}

	public List<KeyValue> constructKeyValuePairs(
			byte[] adapterId,
			T entry ) {

		List<KeyValue> keyValuePairs = new ArrayList<>();
		Key key;
		Value value;
		KeyValue keyValuePair;
		DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				adapter,
				index,
				entry,
				visibilityWriter);
		if (callback != null) {
			if (ingestInfo == null) {
				return Collections.EMPTY_LIST;
			}
			callback.entryIngested(
					ingestInfo,
					entry);
		}
		List<ByteArrayId> rowIds = ingestInfo.getRowIds();
		List<FieldInfo<?>> fieldInfoList = DataStoreUtils.composeFlattenedFields(
				ingestInfo.getFieldInfo(),
				index.getIndexModel(),
				adapter);

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
				keyValuePair = new KeyValue(
						key,
						value);
				keyValuePairs.add(keyValuePair);
			}
		}

		return keyValuePairs;
	}

}
