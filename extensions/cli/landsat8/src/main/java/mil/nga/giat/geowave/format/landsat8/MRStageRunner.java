package mil.nga.giat.geowave.format.landsat8;

import org.opengis.feature.simple.SimpleFeature;

public class MRStageRunner extends
		AnalyzeRunner
{

	public MRStageRunner(
			Landsat8BasicCommandLineOptions landsatOptions ) {
		super(
				landsatOptions);
	}

	@Override
	protected void nextScene(
			SimpleFeature firstBandOfScene,
			AnalysisInfo analysisInfo ) {
		// analysisInfo.entityBandIdToSimpleFeatureMap.
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	@Override
	protected void nextBand(
			SimpleFeature band,
			AnalysisInfo analysisInfo ) {
		super.nextBand(
				band,
				analysisInfo);
	}

	@Override
	protected void lastSceneComplete(
			AnalysisInfo analysisInfo ) {
		super.lastSceneComplete(analysisInfo);
	}

}
