package mil.nga.giat.geowave.datastore.accumulo.cli;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "accumulo", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Utility operations to set accumulo splits")
public class AccumuloSection extends
		DefaultOperation
{

}
