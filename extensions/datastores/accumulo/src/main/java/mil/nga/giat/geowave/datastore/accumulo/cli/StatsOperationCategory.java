package mil.nga.giat.geowave.datastore.accumulo.cli;

import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

public class StatsOperationCategory extends
		CustomOperationCategory
{

	public StatsOperationCategory() {
		super(
				"stats",
				"Stats",
				"Operations to manage statistics in GeoWave");
	}

}