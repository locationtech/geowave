package org.locationtech.geowave.datastore.redis.util;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.redisson.api.RScoredSortedSet;
import org.redisson.client.protocol.ScoredEntry;

public class LazyPaginatedEntryRange<V> extends
		LazyIteratorChain<ScoredEntry<V>>
{
	private double startScore;
	private boolean startScoreInclusive;
	private double endScore;
	private boolean endScoreInclusive;
	private RScoredSortedSet<V> set;
	private Collection<ScoredEntry<V>> currentResult;
	private int currentOffset = 0;

	public LazyPaginatedEntryRange(
			double startScore,
			boolean startScoreInclusive,
			double endScore,
			boolean endScoreInclusive,
			RScoredSortedSet<V> set,
			Collection<ScoredEntry<V>> currentResult ) {
		super();
		this.startScore = startScore;
		this.startScoreInclusive = startScoreInclusive;
		this.endScore = endScore;
		this.endScoreInclusive = endScoreInclusive;
		this.set = set;
		this.currentResult = currentResult;
	}

	@Override
	protected Iterator<? extends ScoredEntry<V>> nextIterator(
			final int count ) {
		// the first iterator should be the initial results
		if (count == 1) {
			return currentResult.iterator();
		}
		// subsequent chained iterators will be obtained from redis
		// pagination
		if ((currentResult.size() < RedisUtils.MAX_ROWS_FOR_PAGINATION)) {
			return null;
		}
		else {
			currentOffset += RedisUtils.MAX_ROWS_FOR_PAGINATION;
			currentResult = set.entryRange(
					startScore,
					startScoreInclusive,
					endScore,
					endScoreInclusive,
					currentOffset,
					RedisUtils.MAX_ROWS_FOR_PAGINATION);
			return currentResult.iterator();
		}
	}
}
