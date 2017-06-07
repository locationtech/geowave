package mil.nga.giat.geowave.format.twitter.stream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import twitter4j.StatusListener;

public class TwitterStreamArchiver
{
	private final static Logger LOGGER = Logger.getLogger(TwitterStreamArchiver.class);

	private long frequency = 5000L; // millis: default = 5 sec
	private int fileSplits = 3;
	private int numProcessingThreads = 4;
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessSecret;
	private String archivePath;
	private boolean writeZipped = false;
	private boolean init = false;

	public TwitterStreamArchiver() {

	}

	public void init(
			Properties twitterProps )
			throws IOException {
		if (twitterProps == null) {
			throw new IOException(
					"Twitter configuration properties required!");
		}

		consumerKey = twitterProps.getProperty("twitter.consumer.key");
		if (consumerKey == null) {
			throw new IOException(
					"Twitter Consumer Key required!");
		}

		consumerSecret = twitterProps.getProperty("twitter.consumer.secret");
		if (consumerSecret == null) {
			throw new IOException(
					"Twitter Consumer Secret required!");
		}

		accessToken = twitterProps.getProperty("twitter.access.token");
		if (accessToken == null) {
			throw new IOException(
					"Twitter Access Token required!");
		}

		accessSecret = twitterProps.getProperty("twitter.access.secret");
		if (accessSecret == null) {
			throw new IOException(
					"Twitter Access Secret required!");
		}

		archivePath = twitterProps.getProperty("twitter.archive.path");
		if (archivePath == null) {
			throw new IOException(
					"Twitter Archive Path required!");
		}

		String fileSplitStr = twitterProps.getProperty("twitter.archive.fileSplits");
		if (fileSplitStr != null) {
			fileSplits = Integer.parseInt(fileSplitStr);
		}

		String pollingFrequencyStr = twitterProps.getProperty("twitter.archive.frequencyMillis");
		if (pollingFrequencyStr != null) {
			frequency = Long.parseLong(pollingFrequencyStr);
		}

		String processingThreadsStr = twitterProps.getProperty("twitter.archive.threads");
		if (processingThreadsStr != null) {
			numProcessingThreads = Integer.parseInt(processingThreadsStr);
		}

		String writeZippedStr = twitterProps.getProperty("twitter.archive.writeZipped");
		if (writeZippedStr != null) {
			writeZipped = Boolean.parseBoolean(writeZippedStr);
		}

		init = true;
	}

	public void run()
			throws InterruptedException,
			IOException {
		if (!init) {
			throw new IOException(
					"TwitterStreamArchiver not initialized!");
		}

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(
				10000);

		TwitterArchiveWriter archiveWriter = new TwitterArchiveFileWriter(
				archivePath,
				fileSplits,
				writeZipped);

		StatusListener statusListener = new TwitterLocationListener(
				archiveWriter);

		// This should be configurable?
		// We can have up to 25 watch boxes
		Location wholeWorld = new Location(
				new Location.Coordinate(
						-180.0,
						-90.0),
				new Location.Coordinate(
						180.0,
						90.0));
		ArrayList<Location> locations = new ArrayList<>();
		locations.add(wholeWorld);

		// Use the filter endpoint
		StatusesFilterEndpoint endpoint = (new StatusesFilterEndpoint()).locations(locations);

		Authentication auth = new OAuth1(
				consumerKey,
				consumerSecret,
				accessToken,
				accessSecret);

		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder().hosts(
				Constants.STREAM_HOST).endpoint(
				endpoint).authentication(
				auth).processor(
				new StringDelimitedProcessor(
						queue)).build();

		// Create an executor service which will spawn threads to do the actual
		// work of parsing the incoming messages and
		// calling the listeners on each message
		ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

		// Wrap our BasicClient with the twitter4j client
		TwitterArchiveClient archiveClient = new TwitterArchiveClient(
				client,
				queue,
				Lists.newArrayList(statusListener),
				service);

		// Establish a connection
		archiveClient.connect();

		while (!archiveClient.isDone()) {
			for (int threads = 0; threads < numProcessingThreads; threads++) {
				// This must be called once per processing thread
				archiveClient.process();
			}

			Thread.sleep(frequency);
		}

		client.stop();
	}

	public static void main(
			String[] args ) {
		String twitterPropsFileName;

		if (args.length < 1) {
			twitterPropsFileName = FileUtils.getUserDirectoryPath() + "/twitter-config.properties";
		}
		else {
			twitterPropsFileName = args[0];
		}

		File twitterPropsFile = new File(
				twitterPropsFileName);
		if (!twitterPropsFile.exists()) {
			LOGGER.error("Unable to locate twitter configuration properties: " + twitterPropsFileName);
			System.exit(-1);
		}

		Properties twitterProps = ConfigOptions.loadProperties(
				twitterPropsFile,
				null);

		TwitterStreamArchiver tsa = new TwitterStreamArchiver();

		try {
			tsa.init(twitterProps);
			tsa.run();
		}
		catch (InterruptedException e) {
			LOGGER.error(e);
		}
		catch (IOException e) {
			LOGGER.error(e);
		}
	}
}