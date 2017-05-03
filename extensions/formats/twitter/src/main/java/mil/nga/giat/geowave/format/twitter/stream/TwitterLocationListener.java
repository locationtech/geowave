package mil.nga.giat.geowave.format.twitter.stream;

import java.io.IOException;

import org.apache.log4j.Logger;

import twitter4j.GeoLocation;
import twitter4j.JSONObject;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterLocationListener implements
		StatusListener
{
	private final static Logger LOGGER = Logger.getLogger(TwitterLocationListener.class);

	private TwitterArchiveWriter archiveWriter;
	private JSONObject currentJson;

	public TwitterLocationListener(
			final TwitterArchiveWriter archiveWriter ) {
		this.archiveWriter = archiveWriter;
	}

	public void setCurrentJson(
			JSONObject json ) {
		this.currentJson = json;
	}

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
		boolean archiveMe = false;

		if (status.getGeoLocation() != null) {
			GeoLocation geo = status.getGeoLocation();
			archiveMe = true;
			LOGGER.debug("Status has GEO at Lat: " + geo.getLatitude() + ", Lon: " + geo.getLongitude());
		}
		else if (status.getPlace() != null) {
			Place place = status.getPlace();
			LOGGER.debug("Status has PLACE called " + place.getFullName());

			if (place.getGeometryType() != null) {
				archiveMe = true;
				LOGGER.debug("  and a geometry type of " + place.getGeometryType());
			}
			else if (place.getBoundingBoxType() != null) {
				archiveMe = true;
				LOGGER.debug("  and a geometry type of " + place.getBoundingBoxType());

				GeoLocation[][] bbox = place.getBoundingBoxCoordinates();
				if (bbox != null && bbox.length > 0) {
					GeoLocation[] poly = bbox[0];
					if (poly != null) {
						for (int i = 0; i < poly.length; i++) {
							GeoLocation vert = poly[i];
							LOGGER.debug(vert.toString());
						}
					}
				}
			}
			else {
				LOGGER.debug("Parsing place some other way...");
			}
		}
		else {
			LOGGER.debug("How did we get here?");
		}

		if (archiveMe) {
			try {
				archiveWriter.writeTweet(
						status,
						currentJson);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void onStallWarning(
			StallWarning stallWarning ) {}

	@Override
	public void onScrubGeo(
			long arg0,
			long arg1 ) {}

	@Override
	public void onDeletionNotice(
			StatusDeletionNotice deletionNotice ) {}
}
