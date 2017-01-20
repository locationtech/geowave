package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
/**
 * Enumeration for Frequency Unit
 */
public enum FrequencyUnitType {

	/**
	 * 1 Terra Hertz = 1,000,000,000,000 Hz (10^12)
	 */
	THz,

	/**
	 * 1 Giga Hertz = 1,000,000,000 Hz (10^9)
	 */
	GHz,

	/**
	 * 1 Mega Hertz = 1,000,000 Hz (10^6)
	 */
	MHz,

	/**
	 * 1 Kilo Hertz = 1,000 Hz (10^3)
	 */
	KHz,

	/**
	 * Hertz (Cycles per second)
	 */
	Hz;
}
