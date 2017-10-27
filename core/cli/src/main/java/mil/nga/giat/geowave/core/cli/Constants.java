/**
 * 
 */
package mil.nga.giat.geowave.core.cli;

/**
 *
 */
public interface Constants
{
	/**
	 * Name of the GeoWave Descriptions Bundle for storing descriptions that
	 * override the CLI descriptions
	 */
	public static final String GEOWAVE_DESCRIPTIONS_BUNDLE_NAME = "GeoWaveLabels";

	/**
	 * Properties file key denoting if a console echo is enabled by default
	 */
	/*
	 * HP Fortify
	 * "Use of Hard-coded Credentials - Key Management: Hardcoded Encryption Key"
	 * false positive This is not an encryption key, just a configuration flag
	 * that denotes if encryption should be enabled in the source.
	 */
	public static final String CONSOLE_DEFAULT_ECHO_ENABLED_KEY = "geowave.console.default.echo.enabled";

	/**
	 * Properties file key denoting if a console echo is enabled for passwords
	 */
	/*
	 * HP Fortify
	 * "Use of Hard-coded Password - Password Management: Hardcoded Password"
	 * false positive This is not a hard-coded password, just a configuration
	 * flag related to passwords, to enable or disable passwords being echoed on
	 * the CLI when a user is entering their password
	 */
	public static final String CONSOLE_PASSWORD_ECHO_ENABLED_KEY = "geowave.console.password.echo.enabled";

	/**
	 * Properties file key denoting if encryption is enabled for passwords
	 */
	public static final String ENCRYPTION_ENABLED_KEY = "geowave.encryption.enabled";

	/**
	 * Default setting for encryption turned on. Currently defaults to disabled.
	 * Must be a boolean string.
	 */
	public static final String ENCRYPTION_ENABLED_DEFAULT = Boolean.TRUE.toString();
}