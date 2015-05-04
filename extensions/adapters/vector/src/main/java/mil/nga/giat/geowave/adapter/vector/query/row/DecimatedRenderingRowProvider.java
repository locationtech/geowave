package mil.nga.giat.geowave.adapter.vector.query.row;

import mil.nga.giat.geowave.core.index.NumericIndexStrategy;

import org.apache.accumulo.core.data.Key;
import org.geotools.geometry.jts.ReferencedEnvelope;

/**
 * This wraps a row ID store as a row provider. It simply checks the underlying
 * row ID store if the current row is painted, and if it is, it finds the next
 * row in sorted order from the row ID store to skip to. This extends
 * DecomposedQueryRangeRowProvider to also provide the capability to decompose
 * the ReferencedEnvelope into query ranges to also use to seek appropriately
 * (intersection of decomposed query ranges and unpainted pixel ranges).
 * 
 */
public class DecimatedRenderingRowProvider extends
		DecomposedQueryRangeRowProvider
{
	private final BasicRowIdStore rowIdStore;

	public DecimatedRenderingRowProvider(
			final BasicRowIdStore rowIdStore,
			final NumericIndexStrategy indexStrategy,
			final ReferencedEnvelope envelope ) {
		super(
				indexStrategy,
				envelope);
		this.rowIdStore = rowIdStore;
	}

	@Override
	public boolean skipRows() {
		if (rowIdStore.isPainted(currentRow)) {
			return true;
		}
		return super.skipRows();
	}

	@Override
	public void setCurrentRow(
			final Key currentRow ) {
		super.setCurrentRow(currentRow);
		rowIdStore.setCurrentRow(currentRow);
	}

	@Override
	public Key getNextRow() {
		final Key rowIdNextRow = rowIdStore.getNextRow(currentRow);
		if (rowIdNextRow == null) {
			return null;
		}
		final Key queryRangeNextRow = super.getNextRow();
		if (queryRangeNextRow == null) {
			return null;
		}
		if (rowIdNextRow.compareTo(queryRangeNextRow) > 0) {
			return rowIdNextRow;
		}
		else {
			return queryRangeNextRow;
		}
	}

}
