package mil.nga.giat.geowave.adapter.vector.export;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "vector", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "TBD")
public class VectorSection extends
		DefaultOperation
{

}
