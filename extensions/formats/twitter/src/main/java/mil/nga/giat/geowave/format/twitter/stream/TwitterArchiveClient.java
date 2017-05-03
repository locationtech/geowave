package mil.nga.giat.geowave.format.twitter.stream;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.twitter.hbc.core.Client;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;

import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.StatusListener;
import twitter4j.TwitterException;

public class TwitterArchiveClient extends
		Twitter4jStatusClient
{
	private final static Logger LOGGER = Logger.getLogger(TwitterArchiveClient.class);

	private TwitterLocationListener listener = null;

	public TwitterArchiveClient(
			final Client client,
			final BlockingQueue<String> blockingQueue,
			final List<? extends StatusListener> listeners,
			final ExecutorService executorService ) {
		super(
				client,
				blockingQueue,
				listeners,
				executorService);

		if (listeners != null) {
			for (StatusListener statusListener : listeners) {
				if (statusListener instanceof TwitterLocationListener) {
					this.listener = (TwitterLocationListener) statusListener;
					break;
				}
			}
		}

		if (this.listener == null) {
			LOGGER.error("No location listener set for twitter archiver");
		}
	}

	@Override
	protected void parseMessage(
			String msg )
			throws JSONException,
			TwitterException,
			IOException {
		if (listener != null) {
			JSONObject json = new JSONObject(
					msg);
			listener.setCurrentJson(json);
		}

		super.parseMessage(msg);
	}
}
