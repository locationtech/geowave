package mil.nga.giat.geowave.core.store.index;

public enum SecondaryIndexType {

	JOIN(
			"JOIN"),
	PARTIAL(
			"PARTIAL"),
	FULL(
			"FULL");

	private String value;

	private SecondaryIndexType(
			String value ) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}