package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;

public interface RowScanner
{
	public boolean isMidRow();

	public List<Cell> nextCellsInRow()
			throws IOException;

	public boolean isDone();

	public List<Cell> currentCellsInRow();

	public Scan getScan();

	public Map<String, Object> getHints();
}
