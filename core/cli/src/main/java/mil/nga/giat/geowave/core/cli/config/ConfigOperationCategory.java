package mil.nga.giat.geowave.core.cli.config;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.AbstractCommandObject;

@Parameters(commandDescription = "Commands that affect local configuration only such as configuring a store or index")
public class ConfigOperationCategory extends
		AbstractCommandObject
{
	@Override
	public String getName() {
		return "config";
	}
}
