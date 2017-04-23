package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "landsat", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Operations to analyze, download, and ingest Landsat 8 imagery publicly available on AWS")
public class Landsat8Section extends
		DefaultOperation
{

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
