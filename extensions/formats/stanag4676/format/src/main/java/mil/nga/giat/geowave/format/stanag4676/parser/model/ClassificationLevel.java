package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
/**
 * Enumeration for Classification Level
 */
public enum ClassificationLevel {
	TOP_SECRET(
			"TOP_SECRET"),
	SECRET(
			"SECRET"),
	CONFIDENTIAL(
			"CONFIDENTIAL"),
	RESTRICTED(
			"RESTRICTED"),
	UNCLASSIFIED(
			"UNCLASSIFIED");

	private String value;

	ClassificationLevel() {
		this.value = "TOP_SECRET";
	}

	ClassificationLevel(
			final String value ) {
		this.value = value;
	}

	public static ClassificationLevel fromString(
			String value ) {
		for (final ClassificationLevel item : ClassificationLevel.values()) {
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
