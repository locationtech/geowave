package org.locationtech.geowave.datastore.redis.util;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedissonClientCache
{
	private static RedissonClientCache singletonInstance;

	public static synchronized RedissonClientCache getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new RedissonClientCache();
		}
		return singletonInstance;
	}

	private final LoadingCache<String, RedissonClient> clientCache = Caffeine
			.newBuilder()
			.build(
					address -> {
						final Config config = new Config();
						config
								.useSingleServer().setConnectTimeout(15000).setTimeout(15000)
								.setAddress(
										address);
						return Redisson
								.create(
										config);
					});

	protected RedissonClientCache() {}

	public synchronized RedissonClient getClient(
			final String address ) {
		return clientCache
				.get(
						address);
	}
}
