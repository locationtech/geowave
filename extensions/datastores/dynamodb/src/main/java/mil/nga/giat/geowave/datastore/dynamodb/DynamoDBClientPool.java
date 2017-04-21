package mil.nga.giat.geowave.datastore.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;

public class DynamoDBClientPool
{
	private static DynamoDBClientPool singletonInstance;
	private static final int DEFAULT_RETRY_THREADS = 4;
	protected static ExecutorService DYNAMO_RETRY_POOL = Executors.newFixedThreadPool(DEFAULT_RETRY_THREADS);

	public static synchronized DynamoDBClientPool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new DynamoDBClientPool();
		}
		return singletonInstance;
	}

	private final Map<DynamoDBOptions, AmazonDynamoDBAsyncClient> clientCache = new HashMap<DynamoDBOptions, AmazonDynamoDBAsyncClient>();

	public synchronized AmazonDynamoDBAsyncClient getClient(
			final DynamoDBOptions options ) {
		AmazonDynamoDBAsyncClient client = clientCache.get(options);
		if (client == null) {
			ClientConfiguration clientConfig = options.getClientConfig();
			if (options.getRegion() == null) {
				client = new AmazonDynamoDBAsyncClient(
						clientConfig).withEndpoint(options.getEndpoint());
			}
			else {
				client = new AmazonDynamoDBAsyncClient(
						clientConfig).withRegion(options.getRegion());
			}
			clientCache.put(
					options,
					client);
		}
		return client;
	}
}
