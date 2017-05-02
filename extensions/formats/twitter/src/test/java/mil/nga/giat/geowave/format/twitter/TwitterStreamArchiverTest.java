package mil.nga.giat.geowave.format.twitter;

import java.io.File;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.format.twitter.stream.TwitterStreamArchiver;

public class TwitterStreamArchiverTest
{
	private static final String TWITTER_CONFIG = "src/test/resources/twitter-config.properties";
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessSecret;
	
	@Before
	public void setup() {
		Properties twitterProps = ConfigOptions.loadProperties(
				new File(TWITTER_CONFIG),
				null);
		
		consumerKey = twitterProps.getProperty("twitter.consumer.key");
		consumerSecret = twitterProps.getProperty("twitter.consumer.secret");
		accessToken = twitterProps.getProperty("twitter.access.token");
		accessSecret = twitterProps.getProperty("twitter.access.secret");
	}
	
	@Test
	public void testStreamArchiver() {
		TwitterStreamArchiver tsa = new TwitterStreamArchiver();
		
		try {
			tsa.run(
					consumerKey,
					consumerSecret,
					accessToken,
					accessSecret);
		}
		catch (InterruptedException e) {
			System.out.println(
					e);
		}
	}
}
