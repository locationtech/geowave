package mil.nga.giat.geowave.datastore.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.beust.jcommander.ParameterException;

public class DynamoDBClientPool
{
	private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBClientPool.class);
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

			if (options.getRegion() == null && (options.getEndpoint() == null || options.getEndpoint().isEmpty())) {
				throw new ParameterException(
						"Compulsory to specify either the region or the endpoint");
			}

			if (options.getRegion() != null && (options.getEndpoint() != null && !options.getEndpoint().isEmpty())) {
				LOGGER.error("Both region and endpoint specified, considering region ");
			}

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
