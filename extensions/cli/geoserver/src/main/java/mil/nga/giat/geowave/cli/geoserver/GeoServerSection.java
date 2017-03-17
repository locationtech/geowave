package mil.nga.giat.geowave.cli.geoserver;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "gs", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands that manage geoserver data stores and layers")
public class GeoServerSection extends
		DefaultOperation
{

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
