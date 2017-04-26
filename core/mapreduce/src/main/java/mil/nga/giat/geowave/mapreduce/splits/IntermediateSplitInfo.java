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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IntermediateSplitInfo implements
		Comparable<IntermediateSplitInfo>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(IntermediateSplitInfo.class);

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

			final byte[] start = rangeLocationPair.getRange().getStartKey();
			final byte[] end = rangeLocationPair.getRange().getEndKey();

			final double cdfStart = stats.cdf(start);

			final double cdfEnd = stats.cdf(end);
			final double expectedEndValue = stats.quantile(cdfStart + ((cdfEnd - cdfStart) * fraction));
			final int maxCardinality = Math.max(
					start.length,
					end.length);

			byte[] bytes = ByteUtils.toBytes(expectedEndValue);
			byte[] splitKey;
			if ((bytes.length < 8) && (bytes.length < maxCardinality)) {
				// prepend with 0
				bytes = expandBytes(
						bytes,
						Math.min(
								8,
								maxCardinality));
			}
			if (bytes.length < maxCardinality) {
				splitKey = new byte[maxCardinality];
				System.arraycopy(
						bytes,
						0,
						splitKey,
						0,
						bytes.length);
			}
			else {
				splitKey = bytes;
			}

			final String location = rangeLocationPair.getLocation();
			final boolean startKeyInclusive = true;
			final boolean endKeyInclusive = false;
			if (new ByteArrayId(
					start).compareTo(new ByteArrayId(
					splitKey)) >= 0 || new ByteArrayId(
					end).compareTo(new ByteArrayId(
					splitKey)) <= 0) {
				splitKey = SplitsProvider.getMidpoint(rangeLocationPair.getRange());
				if (splitKey == null) {
					return null;
				}

				// if you can split the range only by setting the split to the
				// end, but its not inclusive on the end, just clamp this to the
				// start and don't split producing a new pair
				if (Arrays.equals(
						end,
						splitKey) && !rangeLocationPair.getRange().isEndKeyInclusive()) {
					rangeLocationPair = splitsProvider.constructRangeLocationPair(
							splitsProvider.constructRange(
									rangeLocationPair.getRange().getStartKey(),
									rangeLocationPair.getRange().isStartKeyInclusive(),
									splitKey,
									endKeyInclusive),
							location,
							stats.cardinality(
									rangeLocationPair.getRange().getStartKey(),
									splitKey));
					return null;
				}
				// }
			}

			try {
				final RangeLocationPair newPair = splitsProvider.constructRangeLocationPair(
						splitsProvider.constructRange(
								rangeLocationPair.getRange().getStartKey(),
								rangeLocationPair.getRange().isStartKeyInclusive(),
								splitKey,
								endKeyInclusive),
						location,
						stats.cardinality(
								rangeLocationPair.getRange().getStartKey(),
								splitKey));

				rangeLocationPair = splitsProvider.constructRangeLocationPair(
						splitsProvider.constructRange(
								splitKey,
								startKeyInclusive,
								rangeLocationPair.getRange().getEndKey(),
								rangeLocationPair.getRange().isEndKeyInclusive()),
						location,

						stats.cardinality(
								splitKey,
								rangeLocationPair.getRange().getEndKey()));

				return new IndexRangeLocation(
						newPair,
						index);
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.info(
						"Unable to split range",
						ex);
				return null;
			}
		}

		private byte[] expandBytes(
				final byte valueBytes[],
				final int numBytes ) {
			final byte[] bytes = new byte[numBytes];
			int expansion = 0;
			if (numBytes > valueBytes.length) {
				expansion = (numBytes - valueBytes.length);
				for (int i = 0; i < expansion; i++) {
					bytes[i] = 0;
				}
				for (int i = 0; i < valueBytes.length; i++) {
					bytes[expansion + i] = valueBytes[i];
				}
			}
			else {
				for (int i = 0; i < numBytes; i++) {
					bytes[i] = valueBytes[i];
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
			double nextCardinality = currentCardinality + next.rangeLocationPair.getCardinality();
			if (nextCardinality > targetCardinality) {
				final IndexRangeLocation newSplit = next.split(
						statsCache.get(next.index),
						currentCardinality,
						targetCardinality);
				double splitCardinality = next.rangeLocationPair.getCardinality();
				// Stats can have inaccuracies over narrow ranges
				// thus, a split based on statistics may not be found
				if (newSplit != null) {
					splitCardinality += newSplit.rangeLocationPair.getCardinality();
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
				nextCardinality = currentCardinality + splitCardinality;
				if (nextCardinality > targetCardinality) {
					break;
				}
				currentCardinality = nextCardinality;
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

	public synchronized GeoWaveInputSplit toFinalSplit() {
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
		int result = Double.compare(
				thisTotal,
				otherTotal);
		if (result == 0) {
			result = Integer.compare(
					splitInfo.size(),
					o.splitInfo.size());
			if (result == 0) {
				final List<RangeLocationPair> pairs = new ArrayList<>();

				final List<RangeLocationPair> otherPairs = new ArrayList<>();
				double rangeSum = 0;
				double otherSum = 0;
				for (final List<RangeLocationPair> p : splitInfo.values()) {
					pairs.addAll(p);
				}
				for (final List<RangeLocationPair> p : o.splitInfo.values()) {
					otherPairs.addAll(p);
				}

				result = Integer.compare(
						pairs.size(),
						otherPairs.size());
				if (result == 0) {
					for (final RangeLocationPair p : pairs) {
						rangeSum += SplitsProvider.getRangeLength(p.getRange());
					}
					for (final RangeLocationPair p : otherPairs) {
						otherSum += SplitsProvider.getRangeLength(p.getRange());
					}
					result = Double.compare(
							rangeSum,
							otherSum);
					if (result == 0) {
						result = Integer.compare(
								hashCode(),
								o.hashCode());
					}
				}
			}
		}
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((splitInfo == null) ? 0 : splitInfo.hashCode());
		result = (prime * result) + ((splitsProvider == null) ? 0 : splitsProvider.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final IntermediateSplitInfo other = (IntermediateSplitInfo) obj;
		if (splitInfo == null) {
			if (other.splitInfo != null) {
				return false;
			}
		}
		else if (!splitInfo.equals(other.splitInfo)) {
			return false;
		}
		if (splitsProvider == null) {
			if (other.splitsProvider != null) {
				return false;
			}
		}
		else if (!splitsProvider.equals(other.splitsProvider)) {
			return false;
		}
		return true;
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
