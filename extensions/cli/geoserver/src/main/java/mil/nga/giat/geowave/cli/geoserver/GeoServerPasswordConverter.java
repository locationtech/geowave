/**
 * 
 */
package mil.nga.giat.geowave.cli.geoserver;

import mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants;
import mil.nga.giat.geowave.core.cli.converters.PasswordConverter;

/**
 * Converter for GeoServer passwords
 */
public class GeoServerPasswordConverter extends
		PasswordConverter
{
	public GeoServerPasswordConverter(
			String optionName ) {
		super(
				optionName);
		setPropertyKey(GeoServerConstants.GEOSERVER_PASS);
	}
}