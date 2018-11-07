package org.locationtech.geowave.datastore.redis.util;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RedissonClient;
import org.redisson.api.SortOrder;
import org.redisson.api.mapreduce.RCollectionMapReduce;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.ScoredEntry;

public class RedisScoredSetBatchWrapper<V> implements AutoCloseable {
	private RScoredSortedSetAsync<V> currentSet;
	private RBatch currentBatch;
	private RedissonClient client;
	private String setName;

	public RedisScoredSetBatchWrapper(RedissonClient client, String setName) {
		this.setName = setName;
		this.client = client;
	}
//
//	public void executeAsync() {
//		batch.getScoredSortedSet(s);
//	}
//
//	@Override
//	public void close() {
//		batch.executeAsync();
//	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	public Iterator<ScoredEntry<V>> entryRange(double startScore, boolean startScoreInclusive,
			double endScore, boolean endScoreInclusive) {
		return currentSet.entryRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive);
	}
}
