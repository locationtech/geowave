package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class GeodeticPosition extends
		Position
{

	public GeodeticPosition() {
		elevation = 0.0;
	}

	public GeodeticPosition(
			double latitudeDegrees,
			double longitudeDegrees ) {
		latitude = latitudeDegrees;
		longitude = longitudeDegrees;
		elevation = 0.0;
	}

	public GeodeticPosition(
			double latitudeDegrees,
			double longitudeDegrees,
			double elevationMeters ) {
		latitude = latitudeDegrees;
		longitude = longitudeDegrees;
		elevation = elevationMeters;
	}

	/**
	 * latitude in decimal degrees
	 */
	public Double latitude;

	/**
	 * longitude in decimal degrees
	 */
	public Double longitude;

	/**
	 * elevation in meters above ellipsoid (WGS84)
	 */
	public Double elevation;
}
