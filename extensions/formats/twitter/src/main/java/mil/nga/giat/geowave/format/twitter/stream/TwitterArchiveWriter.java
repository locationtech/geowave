package mil.nga.giat.geowave.format.twitter.stream;

import java.io.IOException;

import twitter4j.JSONObject;
import twitter4j.Status;

public interface TwitterArchiveWriter
{
	public void writeTweet(
			Status status,
			JSONObject json )
			throws IOException;
}
