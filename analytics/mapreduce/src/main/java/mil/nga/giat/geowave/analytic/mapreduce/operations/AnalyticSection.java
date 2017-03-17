package mil.nga.giat.geowave.analytic.mapreduce.operations;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "analytic", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands that run mapreduce or spark processing to enhance an existing GeoWave dataset")
public class AnalyticSection extends
		DefaultOperation
{

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
