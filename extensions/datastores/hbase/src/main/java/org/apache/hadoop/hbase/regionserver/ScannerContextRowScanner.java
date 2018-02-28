package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;

import mil.nga.giat.geowave.datastore.hbase.server.RowScanner;

/**
 * this is required to be in org.apache.hadoop.hbase.regionserver because it
 * accesses package private methods within ScannerContext
 *
 *
 */
public class ScannerContextRowScanner implements
		RowScanner
{
	private final InternalScanner scanner;
	private final ScannerContext scannerContext;
	private final List<Cell> cells;
	private boolean done = false;
	private final Scan scan;
	private Map<String, Object> hints;

	public ScannerContextRowScanner(
			final InternalScanner scanner,
			final List<Cell> cells,
			final ScannerContext scannerContext,
			final Scan scan ) {
		this.scanner = scanner;
		this.cells = cells;
		this.scannerContext = scannerContext;
		this.scan = scan;
	}

	@Override
	public boolean isMidRow() {
		if ((scannerContext == null) || done) {
			return false;
		}
		return scannerContext.partialResultFormed();
	}

	@Override
	public List<Cell> nextCellsInRow()
			throws IOException {
		if (!isMidRow()) {
			return Collections.EMPTY_LIST;
		}
		scannerContext.clearProgress();
		scannerContext.setScannerState(NextState.MORE_VALUES);
		done = !scanner.next(
				cells,
				scannerContext);
		return cells;
	}

	@Override
	public List<Cell> currentCellsInRow() {
		return cells;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public Map<String, Object> getHints() {
		if (hints == null) {
			// this isn't threadsafe but shouldn't need to be
			hints = new HashMap<>();
		}
		return hints;
	}

	@Override
	public Scan getScan() {
		return scan;
	}
}
