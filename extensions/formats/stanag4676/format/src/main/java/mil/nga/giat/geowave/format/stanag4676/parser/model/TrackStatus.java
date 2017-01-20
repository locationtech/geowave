package mil.nga.giat.geowave.format.stanag4676.parser.model;

/**
 * Provides the status of a track (i.e. initiating, maintaining, dropping,
 * terminated).
 */
public enum TrackStatus {
	INITIATING(
			"INITIATING"),
	MAINTAINING(
			"MAINTAINING"),
	DROPPING(
			"DROPPING"),
	TERMINATED(
			"TERMINATED");

	private String value;

	TrackStatus() {
		this.value = TrackStatus.values()[0].toString();
	}

	TrackStatus(
			final String value ) {
		this.value = value;
	}

	public static TrackStatus fromString(
			String value ) {
		for (final TrackStatus item : TrackStatus.values()) {
			if (item.toString().equals(
					value)) {
				return item;
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return value;
	}
}