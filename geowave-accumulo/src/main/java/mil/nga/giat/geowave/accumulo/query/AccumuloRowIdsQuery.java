package mil.nga.giat.geowave.accumulo.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.ScannerBase;

/**
 * Represents a query operation for a specific set of Accumulo row IDs.
 * 
 */
public class AccumuloRowIdsQuery extends
		AccumuloFilteredIndexQuery
{

	final Collection<ByteArrayId> rows;

	public AccumuloRowIdsQuery(
			final Index index,
			final Collection<ByteArrayId> rows,
			final String[] authorizations ) {
		super(
				index,
				null,
				authorizations);
		this.rows = rows;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		for (ByteArrayId row : rows)
			ranges.add(new ByteArrayRange(
					row,
					row));
		return ranges;
	}

	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {}
}