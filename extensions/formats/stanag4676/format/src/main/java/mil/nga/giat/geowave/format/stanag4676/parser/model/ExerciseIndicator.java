package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
/**
 * Enumeration Provides an indication of whether the information pertains to
 * operational data, exercise data, or test data.
 */
public enum ExerciseIndicator {
	OPERATIONAL(
			"OPERATIONAL"),
	EXERCISE(
			"EXERCISE"),
	TEST(
			"TEST");

	private String value;

	ExerciseIndicator() {
		this.value = "OPERATIONAL";
	}

	ExerciseIndicator(
			final String value ) {
		this.value = value;
	}

	public static ExerciseIndicator fromString(
			String value ) {
		for (final ExerciseIndicator item : ExerciseIndicator.values()) {
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
