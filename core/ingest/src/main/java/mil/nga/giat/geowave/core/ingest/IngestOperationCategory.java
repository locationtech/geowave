package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

public class IngestOperationCategory extends
		CustomOperationCategory
{

	public IngestOperationCategory() {
		super(
				"ingest",
				"Ingest",
				"Operations to read from common formats and write data to GeoWave");
	}

}
