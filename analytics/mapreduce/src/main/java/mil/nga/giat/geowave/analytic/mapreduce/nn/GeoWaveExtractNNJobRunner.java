package mil.nga.giat.geowave.analytic.mapreduce.nn;

import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;

public class GeoWaveExtractNNJobRunner extends
		NNJobRunner
{

	public GeoWaveExtractNNJobRunner() {
		super();
		setInputFormatConfiguration(new GeoWaveInputFormatConfiguration());
		setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());
		super.setReducerCount(4);
	}

}
