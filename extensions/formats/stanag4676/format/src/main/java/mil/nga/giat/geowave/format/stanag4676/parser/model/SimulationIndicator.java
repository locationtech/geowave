package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
/**
 * Enumeration an indication of whether reported information is real, simulated,
 * or synthesized.
 */
public enum SimulationIndicator {
	/**
	 * Actual sensor collected data
	 */
	REAL(
			"REAL"),

	/**
	 * Computer generated sensor data
	 */
	SIMULATED(
			"SIMULATED"),

	/**
	 * A combination of real and simulated data.
	 */
	SYNTHESIZED(
			"SYNTHESIZED");

	private String value;

	SimulationIndicator() {
		this.value = "REAL";
	}

	SimulationIndicator(
			final String value ) {
		this.value = value;
	}

	public static SimulationIndicator fromString(
			String value ) {
		for (final SimulationIndicator item : SimulationIndicator.values()) {
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
