package mil.nga.giat.geowave.operations.ingest;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "ingest", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands that ingest data directly into GeoWave or stage data to be ingested into GeoWave")
public class IngestSection extends
		DefaultOperation
{
}
