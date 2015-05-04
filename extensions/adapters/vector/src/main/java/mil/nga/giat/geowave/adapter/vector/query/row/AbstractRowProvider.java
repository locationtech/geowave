package mil.nga.giat.geowave.adapter.vector.query.row;

import org.apache.accumulo.core.data.Key;

/**
 * This is a simple abstraction on providing the next row to skip for the
 * RowProviderSkippingIterator to act on. It at least handles setting the
 * current row to a member variable.
 * 
 */
public abstract class AbstractRowProvider
{
	protected Key currentRow;

	public void setCurrentRow(
			final Key currentRow ) {
		this.currentRow = currentRow;
	}

	abstract public boolean skipRows();

	abstract public Key getNextRow();
}
