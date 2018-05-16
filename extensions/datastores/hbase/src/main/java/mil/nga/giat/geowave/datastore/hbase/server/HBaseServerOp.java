package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public interface HBaseServerOp extends
		Persistable
{
	/**
	 *
	 * @param rowScanner
	 *            the cells of the current row, as a scanner so that partial
	 *            cell results within a whole row can be iterated on when a
	 *            single row exceeds internal HBase limits
	 * @return true to continue iteration - false will end the scan, resulting
	 *         in no more subsequent rows (most situations should be true)
	 *
	 * @throws IOException
	 *             e if an exception occurs during iteration
	 */
	public boolean nextRow(
			RowScanner rowScanner )
			throws IOException;

	/**
	 * this is a callback giving an operation that works on scanner scope the
	 * opportunity to effect the scan
	 *
	 * @param scan
	 */
	public void preScannerOpen(
			Scan scan );

	public void init(
			Map<String, String> options )
			throws IOException;
}
