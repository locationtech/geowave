package mil.nga.giat.geowave.adapter.raster.operations;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "raster", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Operations to perform transformations on raster data in GeoWave")
public class RasterSection extends
		DefaultOperation
{

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
