package mil.nga.giat.geowave.format.twitter.stream;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStreamArchiver
{
	private long pollingFrequency = 5000L; // ms

	public TwitterStreamArchiver() {

	}

	private StatusListener statusListener = new StatusListener() {
		@Override
		public void onException(
				Exception ex ) {
			ex.printStackTrace();
		}

		@Override
		public void onTrackLimitationNotice(
				int limit ) {}

		@Override
		public void onStatus(
				Status status ) {

			if (status.getGeoLocation() != null) {
				GeoLocation geo = status.getGeoLocation();
				System.out.println(
						"Status has GEO at Lat: " + geo.getLatitude() + ", Lon: " + geo.getLongitude());
			}
			else if (status.getPlace() != null) {
				Place place = status.getPlace();
				System.out.println(
						"Status has PLACE called " + place.getFullName());

				if (place.getGeometryType() != null) {
					System.out.println(
							"  and a geometry type of " + place.getGeometryType());
				}
				else if (place.getBoundingBoxType() != null) {
					System.out.println(
							"  and a geometry type of " + place.getBoundingBoxType());
					
					GeoLocation[][] bbox = place.getBoundingBoxCoordinates();
					if (bbox != null && bbox.length > 0) {
						GeoLocation[] poly = bbox[0];
						if (poly != null) {
							for (int i = 0; i < poly.length; i++) {
								GeoLocation vert = poly[i];
								System.out.println(vert.toString());
							}
						}
					}
					
					
				}
				else {
					System.out.println("Parsing place some other way...");
				}
			}
			else {
				System.out.println(
						"How did we get here?");
			}
		}

		@Override
		public void onStallWarning(
				StallWarning stallWarning ) {}

		@Override
		public void onScrubGeo(
				long arg0,
				long arg1 ) {
			System.out.println(
					"SCRUB GEO: " + arg0 + ", " + arg1);
		}

		@Override
		public void onDeletionNotice(
				StatusDeletionNotice deletionNotice ) {}
	};

	public void run(
			String consumerKey,
			String consumerSecret,
			String token,
			String secret )
			throws InterruptedException { // Create an appropriately sized
											// blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(
				10000);

		// This should be configurable?
		Location wholeWorld = new Location(
				new Location.Coordinate(
						-180.0,
						-90.0),
				new Location.Coordinate(
						180.0,
						90.0));
		ArrayList<Location> locations = new ArrayList<>();
		locations.add(
				wholeWorld);

		// Use the filter endpoint
		StatusesFilterEndpoint endpoint = (new StatusesFilterEndpoint()).locations(
				locations);

		System.out.println(
				endpoint.getHttpMethod());

		Authentication auth = new OAuth1(
				consumerKey,
				consumerSecret,
				token,
				secret);

		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder()
				.hosts(
						Constants.STREAM_HOST)
				.endpoint(
						endpoint)
				.authentication(
						auth)
				.processor(
						new StringDelimitedProcessor(
								queue))
				.build();

		// Create an executor service which will spawn threads to do the actual
		// work of parsing the incoming messages and
		// calling the listeners on each message
		int numProcessingThreads = 4;
		ExecutorService service = Executors.newFixedThreadPool(
				numProcessingThreads);

		// Wrap our BasicClient with the twitter4j client
		Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
				client,
				queue,
				Lists.newArrayList(
						statusListener),
				service);

		// Establish a connection
		t4jClient.connect();

		while (!t4jClient.isDone()) {
			for (int threads = 0; threads < numProcessingThreads; threads++) {
				// This must be called once per processing thread
				t4jClient.process();
			}

			Thread.sleep(
					pollingFrequency);
		}

		client.stop();
	}
}