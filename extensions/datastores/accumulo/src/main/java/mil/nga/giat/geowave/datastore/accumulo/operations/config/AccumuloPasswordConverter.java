/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import mil.nga.giat.geowave.core.cli.converters.PasswordConverter;

/**
 * Password converter for accumulo password property
 */
public class AccumuloPasswordConverter extends
		PasswordConverter
{
	public AccumuloPasswordConverter(
			String optionName ) {
		super(
				optionName);
	}

	public boolean updatePasswordInConfigs() {
		return false;
	}
}