package mil.nga.giat.geowave.adapter.vector.cli;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "vector", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Vector data operations")
public class VectorSection extends
		DefaultOperation
{

}
