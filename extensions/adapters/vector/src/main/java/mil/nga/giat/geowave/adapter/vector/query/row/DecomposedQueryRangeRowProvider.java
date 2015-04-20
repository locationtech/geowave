package mil.nga.giat.geowave.adapter.vector.query.row;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import com.vividsolutions.jts.geom.Envelope;

/**
 * This class decomposes the given Envelope into a set of Accumulo row ID ranges
 * to use as a row provider to inform seeking to only the given Envelope's
 * decomposed ranges of row IDs.
 * 
 */
public class DecomposedQueryRangeRowProvider extends
		AbstractRowProvider
{
	private final TreeSet<Range> ranges;

	@SuppressFBWarnings(value = {
		"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"
	}, justification = "newer constraint types may be null")
	public DecomposedQueryRangeRowProvider(
			final NumericIndexStrategy indexStrategy,
			final Envelope envelope ) {
		final Constraints constraints = GeometryUtils.basicConstraintsFromEnvelope(envelope);
		if (constraints != null) {
			final List<ByteArrayRange> byteArrayRanges = indexStrategy.getQueryRanges(constraints.getIndexConstraints(indexStrategy));
			ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(byteArrayRanges);
		}
		else {
			ranges = new TreeSet<Range>();
		}
	}

	@Override
	public boolean skipRows() {
		if (!ranges.isEmpty()) {
			if (currentRow == null) {
				return true;
			}
			final Range lowRange = ranges.first();
			if (lowRange.contains(currentRow)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Key getNextRow() {
		if (currentRow != null) {
			// cycle through ranges in order and eliminate any range that
			// precedes the current row
			final Iterator<Range> rangeIt = ranges.iterator();
			while (rangeIt.hasNext()) {
				final Range range = rangeIt.next();
				if (range.afterEndKey(currentRow)) {
					rangeIt.remove();
				}
				else if (range.contains(currentRow)) {
					return currentRow;
				}
				else {
					// the range is after the current row so exit out of this
					// loop and skip to the beginning of this range
					break;
				}
			}
		}
		if (ranges.isEmpty()) {
			return null;
		}
		final Range lowRange = ranges.first();
		return lowRange.getStartKey();
	}
}
