package mil.nga.giat.geowave.cli.osm.operations;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "osm", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Operations to ingest OSM nodes, ways and relations to GeoWave")
public class OSMSection extends
		DefaultOperation
{

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
