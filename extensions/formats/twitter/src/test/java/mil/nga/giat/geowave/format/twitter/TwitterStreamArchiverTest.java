package mil.nga.giat.geowave.format.twitter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.format.twitter.stream.TwitterStreamArchiver;

public class TwitterStreamArchiverTest
{
	private final static Logger LOGGER = Logger.getLogger(TwitterStreamArchiverTest.class);

	private static final String TWITTER_CONFIG = "src/test/resources/twitter-config.properties";
	private Properties twitterProps;
	
	@Before
	public void setup() {
		LOGGER.getRootLogger().setLevel(Level.INFO);
		
		twitterProps = ConfigOptions.loadProperties(
				new File(TWITTER_CONFIG),
				null);
		
	}
	
	@Test
	public void testStreamArchiver() {
		TwitterStreamArchiver tsa = new TwitterStreamArchiver();
		
		try {
			tsa.init(twitterProps);
			tsa.run();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
