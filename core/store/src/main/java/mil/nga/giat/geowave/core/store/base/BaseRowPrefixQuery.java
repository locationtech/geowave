package mil.nga.giat.geowave.core.store.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Represents a query operation using an Accumulo row prefix.
 *
 */
class BaseRowPrefixQuery<T> extends
		AbstractBaseRowQuery<T>
{
	final QueryRanges queryRanges;

	public BaseRowPrefixQuery(
			final PrimaryIndex index,
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix,
			final ScanCallback<T, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				index,
				authorizations,
				scanCallback,
				visibilityCounts);

		final ByteArrayRange sortKeyPrefixRange = new ByteArrayRange(
				sortKeyPrefix,
				sortKeyPrefix,
				false);
		final List<SinglePartitionQueryRanges> ranges = new ArrayList<SinglePartitionQueryRanges>();
		final Collection<ByteArrayRange> sortKeys = Collections.singleton(sortKeyPrefixRange);
		ranges.add(new SinglePartitionQueryRanges(
				partitionKey,
				sortKeys));
		queryRanges = new QueryRanges(
				ranges);
	}

	@Override
	protected QueryRanges getRanges() {
		return queryRanges;
	}

}
