package mil.nga.giat.geowave.example.cli;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "example", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Test and Debug Server Environments for various datastores")
public class ExampleSection extends
		DefaultOperation
{

}
