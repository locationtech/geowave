package mil.nga.giat.geowave.core.cli.operations.config.security;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;

@GeowaveOperation(name = "security", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Configure security & authentication options for GeoWave")
public class SecuritySection extends
		DefaultOperation
{

}