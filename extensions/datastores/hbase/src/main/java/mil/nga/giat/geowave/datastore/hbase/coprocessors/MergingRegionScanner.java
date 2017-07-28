package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergingRegionScanner implements
		RegionScanner
{
	private final static Logger LOGGER = Logger.getLogger(MergingRegionScanner.class);

	private RegionScanner delegate = null;
	private HashMap<ByteArrayId, RowTransform> mergingTransformMap;
	private NextCell peekedCell = null;
	private String scannerId = UUID.randomUUID().toString();

	static class NextCell
	{
		boolean hasMore;
		Cell cell;
	}

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	public MergingRegionScanner(
			final RegionScanner delegate ) {
		this.delegate = delegate;
	}

	public String getId() {
		return scannerId;
	}

	@Override
	public boolean next(
			List<Cell> results,
			ScannerContext scannerContext )
			throws IOException {
		LOGGER.debug(">>> RegionScanner(" + scannerId + ") NEXT(1)...");

		return nextInternal(
				results,
				scannerContext);
	}

	@Override
	public boolean next(
			List<Cell> results )
			throws IOException {
		LOGGER.debug(">>> RegionScanner(" + scannerId + ") NEXT(2)...");

		return nextInternal(
				results,
				null);
	}

	private boolean nextInternal(
			List<Cell> results,
			ScannerContext scannerContext )
			throws IOException {
		NextCell mergedCell = new NextCell();

		LOGGER.debug(">>> RegionScanner(" + scannerId + ") NEXT...");

		if (peekedCell != null) {
			mergedCell.cell = copyCell(
					peekedCell.cell,
					null);
			mergedCell.hasMore = peekedCell.hasMore;

			peekedCell = null;
		}
		else {
			mergedCell = getNextCell();
		}

		// Peek ahead to see if it needs to be merged with the next result
		while (mergedCell.hasMore) {
			peekedCell = getNextCell();
			if (peekedCell.cell != null && CellUtil.matchingRow(
					mergedCell.cell,
					peekedCell.cell)) {
				mergedCell.cell = mergeCells(
						mergedCell.cell,
						peekedCell.cell);
				mergedCell.hasMore = peekedCell.hasMore;
			}
			else {
				break;
			}
		}

		results.clear();
		results.add(mergedCell.cell);

		return mergedCell.hasMore;
	}

	private NextCell getNextCell()
			throws IOException {
		List<Cell> cellList = new ArrayList<>();
		NextCell nextCell = new NextCell();

		boolean hasMore = delegate.next(cellList);

		Cell mergedCell = null;

		for (Cell cell : cellList) {
			if (mergedCell == null) {
				mergedCell = cell;
			}
			else {
				mergedCell = mergeCells(
						mergedCell,
						cell);
			}
		}

		if (mergedCell != null) {
			nextCell.cell = copyCell(
					mergedCell,
					null);
			nextCell.hasMore = hasMore;
		}

		return nextCell;
	}

	/**
	 * Assumes cells share family and qualifier
	 * 
	 * @param cell1
	 * @param cell2
	 * @return
	 */
	private Cell mergeCells(
			Cell cell1,
			Cell cell2 ) {
		Cell mergedCell = null;

		ByteArrayId family = new ByteArrayId(
				CellUtil.cloneFamily(cell1));

		if (mergingTransformMap.containsKey(family)) {
			RowTransform transform = mergingTransformMap.get(family);

			final Mergeable mergeable = transform.getRowAsMergeableObject(
					family,
					new ByteArrayId(
							CellUtil.cloneQualifier(cell1)),
					CellUtil.cloneValue(cell1));

			if (mergeable != null) {
				final Mergeable otherMergeable = transform.getRowAsMergeableObject(
						family,
						new ByteArrayId(
								CellUtil.cloneQualifier(cell1)),
						CellUtil.cloneValue(cell2));

				if (otherMergeable != null) {
					mergeable.merge(otherMergeable);
					LOGGER.warn("MERGE! EVERYBODY MERGE!");
				}
				else {
					LOGGER.error("Cell value is not Mergeable!");
				}

				mergedCell = copyCell(
						cell1,
						PersistenceUtils.toBinary(mergeable));
			}
			else {
				LOGGER.error("Cell value is not Mergeable!");
			}
		}
		else {
			LOGGER.error("No merging transform for adapter: " + family.getString());
		}

		return mergedCell;
	}

	private Cell copyCell(
			Cell sourceCell,
			byte[] newValue ) {
		if (newValue == null) {
			newValue = CellUtil.cloneValue(sourceCell);
		}

		Cell newCell = CellUtil.createCell(
				CellUtil.cloneRow(sourceCell),
				CellUtil.cloneFamily(sourceCell),
				CellUtil.cloneQualifier(sourceCell),
				sourceCell.getTimestamp(),
				KeyValue.Type.Put.getCode(),
				newValue);

		return newCell;
	}

	@Override
	public void close()
			throws IOException {
		delegate.close();
	}

	public void setTransformMap(
			HashMap<ByteArrayId, RowTransform> mergingTransformMap ) {
		this.mergingTransformMap = mergingTransformMap;
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return delegate.getRegionInfo();
	}

	@Override
	public boolean isFilterDone()
			throws IOException {
		return delegate.isFilterDone();
	}

	@Override
	public boolean reseek(
			byte[] row )
			throws IOException {
		return delegate.reseek(row);
	}

	@Override
	public long getMaxResultSize() {
		return delegate.getMaxResultSize();
	}

	@Override
	public long getMvccReadPoint() {
		return delegate.getMvccReadPoint();
	}

	@Override
	public int getBatch() {
		return delegate.getBatch();
	}

	@Override
	public boolean nextRaw(
			List<Cell> result )
			throws IOException {
		LOGGER.debug(">>> RegionScanner(" + scannerId + ") NEXTRAW...");

		return next(result);
	}

	@Override
	public boolean nextRaw(
			List<Cell> result,
			ScannerContext scannerContext )
			throws IOException {
		LOGGER.debug(">>> RegionScanner(" + scannerId + ") NEXTRAW...");

		return next(
				result,
				scannerContext);
	}
}
