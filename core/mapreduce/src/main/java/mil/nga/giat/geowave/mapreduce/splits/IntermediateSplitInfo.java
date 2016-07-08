package mil.nga.giat.geowave.mapreduce.splits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IntermediateSplitInfo implements
		Comparable<IntermediateSplitInfo>
{

	private final static Logger LOGGER = Logger.getLogger(IntermediateSplitInfo.class);

	protected class IndexRangeLocation
	{
		private RangeLocationPair rangeLocationPair;
		private final PrimaryIndex index;

		public IndexRangeLocation(
				final RangeLocationPair rangeLocationPair,
				final PrimaryIndex index ) {
			this.rangeLocationPair = rangeLocationPair;
			this.index = index;
		}

		public IndexRangeLocation split(
				final RowRangeHistogramStatistics<?> stats,
				final double currentCardinality,
				final double targetCardinality ) {

			if (stats == null) {
				return null;
			}

			final double thisCardinalty = rangeLocationPair.getCardinality();
			final double fraction = (targetCardinality - currentCardinality) / thisCardinalty;
			final int splitCardinality = (int) (thisCardinalty * fraction);

			final byte[] start = rangeLocationPair.getRange().getStartKey();
			final byte[] end = rangeLocationPair.getRange().getEndKey();

			final double cdfStart = stats.cdf(start);
			final double cdfEnd = stats.cdf(end);
			final byte[] expectedEnd = stats.quantile(cdfStart + ((cdfEnd - cdfStart) * fraction));

			final int maxCardinality = Math.max(
					start.length,
					end.length);

			final byte[] splitKey = expandBytes(
					expectedEnd,
					maxCardinality);

			if (Arrays.equals(
					start,
					splitKey) || Arrays.equals(
					end,
					splitKey)) {
				return null;
			}

			final String location = rangeLocationPair.getLocation();
			try {
				final RangeLocationPair newPair = splitsProvider.constructRangeLocationPair(
						splitsProvider.constructRange(
								rangeLocationPair.getRange().getStartKey(),
								rangeLocationPair.getRange().isStartKeyInclusive(),
								splitKey,
								false),
						location,
						splitCardinality);

				rangeLocationPair = splitsProvider.constructRangeLocationPair(
						splitsProvider.constructRange(
								splitKey,
								true,
								rangeLocationPair.getRange().getEndKey(),
								rangeLocationPair.getRange().isEndKeyInclusive()),
						location,
						rangeLocationPair.getCardinality() - splitCardinality);

				return new IndexRangeLocation(
						newPair,
						index);
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.info("Unable to split range: " + ex.getLocalizedMessage());
				return null;
			}
		}

		private byte[] expandBytes(
				final byte valueBytes[],
				final int numBytes ) {
			final byte[] bytes = new byte[numBytes];
			for (int i = 0; i < numBytes; i++) {
				if (i < valueBytes.length) {
					bytes[i] = valueBytes[i];
				}
				else {
					bytes[i] = 0;
				}
			}
			return bytes;
		}
	}

	private final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo;
	private final SplitsProvider splitsProvider;

	public IntermediateSplitInfo(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final SplitsProvider splitsProvider ) {
		this.splitInfo = splitInfo;
		this.splitsProvider = splitsProvider;
	}

	synchronized void merge(
			final IntermediateSplitInfo split ) {
		for (final Entry<PrimaryIndex, List<RangeLocationPair>> e : split.splitInfo.entrySet()) {
			List<RangeLocationPair> thisList = splitInfo.get(e.getKey());
			if (thisList == null) {
				thisList = new ArrayList<RangeLocationPair>();
				splitInfo.put(
						e.getKey(),
						thisList);
			}
			thisList.addAll(e.getValue());
		}
	}

	/**
	 * Side effect: Break up this split.
	 *
	 * Split the ranges into two
	 *
	 * @return the new split.
	 */
	synchronized IntermediateSplitInfo split(
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache ) {
		// generically you'd want the split to be as limiting to total
		// locations as possible and then as limiting as possible to total
		// indices, but in this case split() is only called when all ranges
		// are in the same location and the same index

		final TreeSet<IndexRangeLocation> orderedSplits = new TreeSet<IndexRangeLocation>(
				new Comparator<IndexRangeLocation>() {

					@Override
					public int compare(
							final IndexRangeLocation o1,
							final IndexRangeLocation o2 ) {
						return (o1.rangeLocationPair.getCardinality() - o2.rangeLocationPair.getCardinality()) < 0 ? -1
								: 1;
					}
				});
		for (final Entry<PrimaryIndex, List<RangeLocationPair>> ranges : splitInfo.entrySet()) {
			for (final RangeLocationPair p : ranges.getValue()) {
				orderedSplits.add(new IndexRangeLocation(
						p,
						ranges.getKey()));
			}
		}
		final double targetCardinality = getTotalRangeAtCardinality() / 2;
		double currentCardinality = 0.0;
		final Map<PrimaryIndex, List<RangeLocationPair>> otherSplitInfo = new HashMap<PrimaryIndex, List<RangeLocationPair>>();

		splitInfo.clear();

		do {
			final IndexRangeLocation next = orderedSplits.pollFirst();
			final double nextCardinality = currentCardinality + next.rangeLocationPair.getCardinality();
			if (nextCardinality > targetCardinality) {
				final IndexRangeLocation newSplit = next.split(
						statsCache.get(next.index),
						currentCardinality,
						targetCardinality);
				// Stats can have inaccuracies over narrow ranges
				// thus, a split based on statistics may not be found
				if (newSplit != null) {
					addPairForIndex(
							otherSplitInfo,
							newSplit.rangeLocationPair,
							newSplit.index);
					addPairForIndex(
							splitInfo,
							next.rangeLocationPair,
							next.index);
				}
				else {
					// Still add to the other SPLIT if there is remaining
					// pairs
					// in this SPLIT
					addPairForIndex(
							(!orderedSplits.isEmpty()) ? otherSplitInfo : splitInfo,
							next.rangeLocationPair,
							next.index);
				}

				break;
			}
			else {
				addPairForIndex(
						otherSplitInfo,
						next.rangeLocationPair,
						next.index);
				currentCardinality = nextCardinality;
			}
		}
		while (!orderedSplits.isEmpty());

		// What is left of the ranges
		// that haven't been placed in the other split info

		for (final IndexRangeLocation split : orderedSplits) {
			addPairForIndex(
					splitInfo,
					split.rangeLocationPair,
					split.index);
		}
		// All ranges consumed by the other split
		if (splitInfo.size() == 0) {
			// First try to move a index set of ranges back.
			if (otherSplitInfo.size() > 1) {
				final Iterator<Entry<PrimaryIndex, List<RangeLocationPair>>> it = otherSplitInfo.entrySet().iterator();
				final Entry<PrimaryIndex, List<RangeLocationPair>> entry = it.next();
				it.remove();
				splitInfo.put(
						entry.getKey(),
						entry.getValue());
			}
			else {
				splitInfo.putAll(otherSplitInfo);
				otherSplitInfo.clear();
			}
		}

		return otherSplitInfo.size() == 0 ? null : new IntermediateSplitInfo(
				otherSplitInfo,
				splitsProvider);
	}

	private void addPairForIndex(
			final Map<PrimaryIndex, List<RangeLocationPair>> otherSplitInfo,
			final RangeLocationPair pair,
			final PrimaryIndex index ) {
		List<RangeLocationPair> list = otherSplitInfo.get(index);
		if (list == null) {
			list = new ArrayList<RangeLocationPair>();
			otherSplitInfo.put(
					index,
					list);
		}
		list.add(pair);

	}

	synchronized GeoWaveInputSplit toFinalSplit() {
		final Set<String> locations = new HashSet<String>();
		for (final Entry<PrimaryIndex, List<RangeLocationPair>> entry : splitInfo.entrySet()) {
			for (final RangeLocationPair pair : entry.getValue()) {
				locations.add(pair.getLocation());
			}
		}
		return splitsProvider.constructInputSplit(
				splitInfo,
				locations.toArray(new String[locations.size()]));
	}

	@Override
	public int compareTo(
			final IntermediateSplitInfo o ) {
		final double thisTotal = getTotalRangeAtCardinality();
		final double otherTotal = o.getTotalRangeAtCardinality();
		return (thisTotal - otherTotal) < 0 ? -1 : 1;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof IntermediateSplitInfo)) {
			return false;
		}
		return compareTo((IntermediateSplitInfo) obj) == 0;
	}

	@Override
	public int hashCode() {
		// think this matches the spirit of compareTo
		return com.google.common.base.Objects.hashCode(
				getTotalRangeAtCardinality(),
				super.hashCode());
	}

	private synchronized double getTotalRangeAtCardinality() {
		double sum = 0.0;
		for (final List<RangeLocationPair> pairList : splitInfo.values()) {
			for (final RangeLocationPair pair : pairList) {
				sum += pair.getCardinality();
			}
		}
		return sum;
	}
}
