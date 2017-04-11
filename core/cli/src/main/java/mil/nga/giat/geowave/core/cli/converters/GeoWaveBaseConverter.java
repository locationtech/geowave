/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import com.beust.jcommander.converters.BaseConverter;

/**
 * Base value converter for handling field conversions of varying types
 * @param <T>
 */
public abstract class GeoWaveBaseConverter<T> extends BaseConverter<T> {

	public GeoWaveBaseConverter(String optionName) {
		super(optionName);
	}

	/**
	 * Specify if a converter is for a password field. This allows a password field to be specified, though 
	 * side-stepping most of the default jcommander password functionality
	 * @return
	 */
	abstract public boolean isPassword();
}
