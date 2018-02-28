package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import mil.nga.giat.geowave.core.index.ByteArrayRange.MergeOperation;

public class QueryRanges
{

	private final Collection<SinglePartitionQueryRanges> partitionRanges;
	private List<ByteArrayRange> compositeQueryRanges;

	public QueryRanges() {
		// this implies an infinite range
		partitionRanges = null;
	}

	public QueryRanges(
			final Set<ByteArrayId> partitionKeys,
			final QueryRanges queryRanges ) {
		if ((queryRanges == null) || (queryRanges.partitionRanges == null) || queryRanges.partitionRanges.isEmpty()) {
			partitionRanges = fromPartitionKeys(partitionKeys);
		}
		else if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			partitionRanges = queryRanges.partitionRanges;
		}
		else {
			partitionRanges = new ArrayList<>(
					partitionKeys.size() * queryRanges.partitionRanges.size());
			for (final ByteArrayId partitionKey : partitionKeys) {
				for (final SinglePartitionQueryRanges sortKeyRange : queryRanges.partitionRanges) {
					ByteArrayId newPartitionKey;
					if (partitionKey == null) {
						newPartitionKey = sortKeyRange.getPartitionKey();
					}
					else if (sortKeyRange.getPartitionKey() == null) {
						newPartitionKey = partitionKey;
					}
					else {
						newPartitionKey = new ByteArrayId(
								ByteArrayUtils.combineArrays(
										partitionKey.getBytes(),
										sortKeyRange.getPartitionKey().getBytes()));
					}
					partitionRanges.add(new SinglePartitionQueryRanges(
							newPartitionKey,
							sortKeyRange.getSortKeyRanges()));
				}
			}
		}
	}

	public QueryRanges(
			final List<QueryRanges> queryRangesList ) {
		// group by partition
		final Map<ByteArrayId, Collection<ByteArrayRange>> sortRangesPerPartition = new HashMap<>();
		for (final QueryRanges qr : queryRangesList) {
			for (final SinglePartitionQueryRanges r : qr.getPartitionQueryRanges()) {
				final Collection<ByteArrayRange> ranges = sortRangesPerPartition.get(r.getPartitionKey());
				if (ranges == null) {
					sortRangesPerPartition.put(
							r.getPartitionKey(),
							r.getSortKeyRanges());
				}
				else {
					ranges.addAll(r.getSortKeyRanges());
				}
			}
		}
		partitionRanges = new ArrayList<>(
				sortRangesPerPartition.size());
		for (final Entry<ByteArrayId, Collection<ByteArrayRange>> e : sortRangesPerPartition.entrySet()) {
			Collection<ByteArrayRange> mergedRanges;
			if (e.getValue() != null) {
				mergedRanges = ByteArrayRange.mergeIntersections(
						e.getValue(),
						MergeOperation.UNION);
			}
			else {
				mergedRanges = null;
			}
			partitionRanges.add(new SinglePartitionQueryRanges(
					e.getKey(),
					mergedRanges));
		}
	}

	public QueryRanges(
			final Collection<SinglePartitionQueryRanges> partitionRanges ) {
		this.partitionRanges = partitionRanges;
	}

	public QueryRanges(
			final ByteArrayRange singleSortKeyRange ) {
		partitionRanges = Collections.singletonList(new SinglePartitionQueryRanges(
				singleSortKeyRange));
	}

	public QueryRanges(
			final Set<ByteArrayId> partitionKeys ) {
		partitionRanges = fromPartitionKeys(partitionKeys);
	}

	private static Collection<SinglePartitionQueryRanges> fromPartitionKeys(
			final Set<ByteArrayId> partitionKeys ) {
		if (partitionKeys == null) {
			return null;
		}
		return Collections2.transform(
				partitionKeys,
				new Function<ByteArrayId, SinglePartitionQueryRanges>() {
					@Override
					public SinglePartitionQueryRanges apply(
							final ByteArrayId input ) {
						return new SinglePartitionQueryRanges(
								input);
					}
				});
	}

	public Collection<SinglePartitionQueryRanges> getPartitionQueryRanges() {
		return partitionRanges;
	}

	public List<ByteArrayRange> getCompositeQueryRanges() {
		if (partitionRanges == null) {
			return null;
		}
		if (compositeQueryRanges != null) {
			return compositeQueryRanges;
		}
		if (partitionRanges.isEmpty()) {
			compositeQueryRanges = new ArrayList<>();
			return compositeQueryRanges;
		}
		final List<ByteArrayRange> internalQueryRanges = new ArrayList<>();
		for (final SinglePartitionQueryRanges partition : partitionRanges) {
			if ((partition.getSortKeyRanges() == null) || partition.getSortKeyRanges().isEmpty()) {
				internalQueryRanges.add(new ByteArrayRange(
						partition.getPartitionKey(),
						partition.getPartitionKey(),
						true));
			}

			else if (partition.getPartitionKey() == null) {
				internalQueryRanges.addAll(partition.getSortKeyRanges());
			}
			else {
				for (final ByteArrayRange sortKeyRange : partition.getSortKeyRanges()) {
					internalQueryRanges.add(new ByteArrayRange(
							new ByteArrayId(
									ByteArrayUtils.combineArrays(
											partition.getPartitionKey().getBytes(),
											sortKeyRange.getStart().getBytes())),
							new ByteArrayId(
									ByteArrayUtils.combineArrays(
											partition.getPartitionKey().getBytes(),
											sortKeyRange.getEnd().getBytes())),
							sortKeyRange.singleValue));
				}
			}
		}

		compositeQueryRanges = internalQueryRanges;
		return compositeQueryRanges;
	}

	public boolean isMultiRange() {
		if (compositeQueryRanges != null) {
			return compositeQueryRanges.size() >= 2;
		}
		if (partitionRanges.isEmpty()) {
			return false;
		}
		if (partitionRanges.size() > 1) {
			return true;
		}
		final SinglePartitionQueryRanges partition = partitionRanges.iterator().next();
		if ((partition.getSortKeyRanges() != null) && (partition.getSortKeyRanges().size() <= 1)) {
			return false;
		}
		return true;
	}
}
