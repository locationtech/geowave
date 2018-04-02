package mil.nga.giat.geowave.datastore.hbase.filters;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

public class HBaseMergingFilter extends
		FilterBase
{
	public HBaseMergingFilter() {}

	public static HBaseMergingFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		HBaseMergingFilter mergingFilter = new HBaseMergingFilter();

		return mergingFilter;
	}

	/**
	 * Enable filterRowCells
	 */
	@Override
	public boolean hasFilterRow() {
		return true;
	}

	/**
	 * Handle the entire row at one time
	 */
	@Override
	public void filterRowCells(
			List<Cell> rowCells )
			throws IOException {
		if (!rowCells.isEmpty()) {
			if (rowCells.size() > 1) {
				Cell firstCell = rowCells.get(0);
				byte[] singleRow = CellUtil.cloneRow(firstCell);
				byte[] singleFam = CellUtil.cloneFamily(firstCell);
				byte[] singleQual = CellUtil.cloneQualifier(firstCell);

				Mergeable mergedValue = null;
				for (Cell cell : rowCells) {
					byte[] byteValue = CellUtil.cloneValue(cell);
					Mergeable value = (Mergeable) PersistenceUtils.fromBinary(byteValue);

					if (mergedValue != null) {
						mergedValue.merge(value);
					}
					else {
						mergedValue = value;
					}
				}

				Cell singleCell = CellUtil.createCell(
						singleRow,
						singleFam,
						singleQual,
						System.currentTimeMillis(),
						KeyValue.Type.Put.getCode(),
						PersistenceUtils.toBinary(mergedValue));

				rowCells.clear();
				rowCells.add(singleCell);
			}
		}
	}

	/**
	 * Don't do anything special here, since we're only interested in whole rows
	 */
	@Override
	public ReturnCode filterKeyValue(
			Cell cell )
			throws IOException {
		return ReturnCode.INCLUDE;
	}
}
