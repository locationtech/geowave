/**
 * 
 */
package mil.nga.giat.geowave.cli.geoserver;

import com.beust.jcommander.Parameters;

/**
 *
 */
@Parameters(commandDescription = "SSL Configuration Options that can be specified if connecting to geoserver over SSL")
public class GeoServerSSLConfigurationOptions extends
		StoreSSLConfigurationOptions
{
	public GeoServerSSLConfigurationOptions() {
		super(
				"geoserver");
	}
}
