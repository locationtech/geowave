package mil.nga.giat.geowave.core.cli.operations.config;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "config", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands that affect local configuration only")
public class ConfigSection extends
		DefaultOperation
{

}