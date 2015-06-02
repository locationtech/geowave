package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloKeyValuePairGenerator </code> Since HBase already has
 *         a concept of Cell, we are using it rather than custom implementation
 *         of KeyValue Pair
 */
public class HBaseCellGenerator<T>
{

	private WritableDataAdapter<T> adapter;
	private Index index;
	private VisibilityWriter<T> visibilityWriter;

	public HBaseCellGenerator(
			WritableDataAdapter<T> adapter,
			Index index,
			VisibilityWriter<T> visibilityWriter ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.visibilityWriter = visibilityWriter;
	}

	public List<Cell> constructKeyValuePairs(
			byte[] adapterId,
			T entry ) {

		List<Cell> keyValuePairs = new ArrayList<>();
		Cell cell;
		DataStoreEntryInfo ingestInfo = HBaseUtils.getIngestInfo(
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
				cell = CellUtil.createCell(
						rowId.getBytes(),
						adapterId,
						fieldInfo.getDataValue().getId().getBytes(),
						System.currentTimeMillis(),
						KeyValue.Type.Put.getCode(),
						fieldInfo.getWrittenValue());
				keyValuePairs.add(cell);
			}
		}

		return keyValuePairs;
	}

}
