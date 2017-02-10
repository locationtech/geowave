package mil.nga.giat.geowave.format.landsat8;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.format.landsat8.WRS2GeometryStore.WRS2Key;

public class AnalyzeRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AnalyzeRunner.class);
	protected Landsat8BasicCommandLineOptions landsatOptions = new Landsat8BasicCommandLineOptions();

	public AnalyzeRunner(
			final Landsat8BasicCommandLineOptions landsatOptions ) {
		this.landsatOptions = landsatOptions;
	}

	protected void runInternal(
			final OperationParams params )
			throws Exception {
		try {
			try (BandFeatureIterator bands = new BandFeatureIterator(
					landsatOptions.isOnlyScenesSinceLastRun(),
					landsatOptions.isUseCachedScenes(),
					landsatOptions.isNBestPerSpatial(),
					landsatOptions.getNBestScenes(),
					landsatOptions.getNBestBands(),
					landsatOptions.getCqlFilter(),
					landsatOptions.getWorkspaceDir())) {
				final AnalysisInfo info = new AnalysisInfo();
				String prevEntityId = null;
				while (bands.hasNext()) {
					final SimpleFeature band = bands.next();
					final String entityId = (String) band.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME);
					if ((prevEntityId == null) || !prevEntityId.equals(entityId)) {
						prevEntityId = entityId;
						nextScene(
								band,
								info);
					}
					nextBand(
							band,
							info);
				}
				lastSceneComplete(info);
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"",
					e);
		}
	}

	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		analysisInfo.nextScene(firstBandOfScene);
	}

	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		analysisInfo.addBandInfo(band);
	}

	protected void lastSceneComplete(
			final AnalysisInfo analysisInfo ) {
		analysisInfo.printSceneInfo();
		analysisInfo.printTotals();
	}

	protected static class AnalysisInfo
	{
		private final TreeMap<String, Float> bandIdToMbMap = new TreeMap<String, Float>();
		private final TreeMap<String, SimpleFeature> entityBandIdToSimpleFeatureMap = new TreeMap<String, SimpleFeature>();
		private int sceneCount = 0;
		private final Set<WRS2Key> wrs2Keys = new HashSet<WRS2Key>();
		private int minRow = Integer.MAX_VALUE;
		private int minPath = Integer.MAX_VALUE;
		private int maxRow = Integer.MIN_VALUE;
		private int maxPath = Integer.MIN_VALUE;
		private double minLat = Double.MAX_VALUE;
		private double minLon = Double.MAX_VALUE;
		private double maxLat = -Double.MAX_VALUE;
		private double maxLon = -Double.MAX_VALUE;
		private long startDate = Long.MAX_VALUE;
		private long endDate = 0;
		private float totalCloudCover = 0f;
		private float minCloudCover = Float.MAX_VALUE;
		private float maxCloudCover = -Float.MAX_VALUE;
		private final Map<String, Integer> processingLevelCounts = new HashMap<String, Integer>();

		private void nextScene(
				final SimpleFeature currentBand ) {
			printSceneInfo();
			sceneCount++;
			entityBandIdToSimpleFeatureMap.clear();
			final int path = (int) currentBand.getAttribute(SceneFeatureIterator.PATH_ATTRIBUTE_NAME);
			final int row = (int) currentBand.getAttribute(SceneFeatureIterator.ROW_ATTRIBUTE_NAME);
			final float cloudCover = (float) currentBand.getAttribute(SceneFeatureIterator.CLOUD_COVER_ATTRIBUTE_NAME);
			final String processingLevel = (String) currentBand
					.getAttribute(SceneFeatureIterator.PROCESSING_LEVEL_ATTRIBUTE_NAME);
			final Date date = (Date) currentBand.getAttribute(SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME);
			minRow = Math.min(
					minRow,
					row);
			maxRow = Math.max(
					maxRow,
					row);
			minPath = Math.min(
					minPath,
					path);
			maxPath = Math.max(
					maxPath,
					path);
			final Envelope env = ((Geometry) currentBand.getDefaultGeometry()).getEnvelopeInternal();
			minLat = Math.min(
					minLat,
					env.getMinY());
			maxLat = Math.max(
					maxLat,
					env.getMaxY());
			minLon = Math.min(
					minLon,
					env.getMinX());
			maxLon = Math.max(
					maxLon,
					env.getMaxX());

			minCloudCover = Math.min(
					minCloudCover,
					cloudCover);
			maxCloudCover = Math.max(
					maxCloudCover,
					cloudCover);
			totalCloudCover += cloudCover;

			Integer count = processingLevelCounts.get(processingLevel);
			if (count == null) {
				count = 0;
			}
			count++;
			processingLevelCounts.put(
					processingLevel,
					count);

			startDate = Math.min(
					startDate,
					date.getTime());
			endDate = Math.max(
					endDate,
					date.getTime());
			wrs2Keys.add(new WRS2Key(
					path,
					row));

		}

		private void printSceneInfo() {
			if (sceneCount > 0) {
				final SimpleDateFormat sdf = new SimpleDateFormat(
						SceneFeatureIterator.AQUISITION_DATE_FORMAT);
				boolean first = true;
				for (final Entry<String, SimpleFeature> entry : entityBandIdToSimpleFeatureMap.entrySet()) {
					final String bandId = entry.getKey();
					final SimpleFeature feature = entry.getValue();
					if (first) {
						if (feature == null) {
							throw new RuntimeException(
									"feature is null");
						}
						// print scene info
						System.out.println("\n<--   "
								+ feature.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME) + "   -->");
						System.out
								.println("Acquisition Date: "
										+ sdf.format(feature
												.getAttribute(SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME)));
						System.out.println("Cloud Cover: "
								+ feature.getAttribute(SceneFeatureIterator.CLOUD_COVER_ATTRIBUTE_NAME));
						System.out.println("Scene Download URL: "
								+ feature.getAttribute(SceneFeatureIterator.SCENE_DOWNLOAD_ATTRIBUTE_NAME));
						first = false;
					}
					final float mb = (Float) feature.getAttribute(BandFeatureIterator.SIZE_ATTRIBUTE_NAME);
					final String bandDownloadUrl = (String) feature
							.getAttribute(BandFeatureIterator.BAND_DOWNLOAD_ATTRIBUTE_NAME);
					// print band info
					System.out.println("Band " + bandId + ": " + mb + " MB, download at " + bandDownloadUrl);
					Float totalMb = bandIdToMbMap.get(bandId);
					if (totalMb == null) {
						totalMb = 0.0f;
					}
					totalMb += mb;
					bandIdToMbMap.put(
							bandId,
							totalMb);
				}
			}
		}

		private void addBandInfo(
				final SimpleFeature band ) {
			entityBandIdToSimpleFeatureMap.put(
					(String) band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME),
					band);
		}

		private void printTotals() {
			System.out.println("\n<--   Totals   -->");
			System.out.println("Total Scenes: " + sceneCount);
			if (sceneCount > 0) {
				final SimpleDateFormat sdf = new SimpleDateFormat(
						SceneFeatureIterator.AQUISITION_DATE_FORMAT);
				System.out.println("Date Range: [" + sdf.format(new Date(
						startDate)) + ", " + sdf.format(new Date(
						endDate)) + "]");
				System.out.println("Cloud Cover Range: [" + minCloudCover + ", " + maxCloudCover + "]");
				System.out.println("Average Cloud Cover: " + (totalCloudCover / sceneCount));
				System.out.println("WRS2 Paths/Rows covered: " + wrs2Keys.size());
				System.out.println("Row Range: [" + minRow + ", " + maxRow + "]");
				System.out.println("Path Range: [" + minPath + ", " + maxPath + "]");
				System.out.println("Latitude Range: [" + minLat + ", " + maxLat + "]");
				System.out.println("Longitude Range: [" + minLon + ", " + maxLon + "]");
				final StringBuffer strBuf = new StringBuffer(
						"Processing Levels: ");
				boolean includeSceneCount = false;
				boolean first = true;
				if (processingLevelCounts.size() > 1) {
					includeSceneCount = true;
				}
				for (final Entry<String, Integer> entry : processingLevelCounts.entrySet()) {
					if (!first) {
						strBuf.append(", ");
					}
					else {
						first = false;
					}
					strBuf.append(entry.getKey());
					if (includeSceneCount) {
						strBuf.append(" (" + entry.getValue() + " scenes)");
					}
				}
				for (final Entry<String, Float> entry : bandIdToMbMap.entrySet()) {
					final String bandId = entry.getKey();
					final float mb = Math.round(entry.getValue() * 10) / 10f;
					final String avg;
					if (sceneCount > 1) {
						avg = "(avg. " + (Math.round((entry.getValue() * 10) / sceneCount) / 10f) + " MB)";
					}
					else {
						avg = "";
					}
					System.out.println("Band " + bandId + ": " + mb + " MB " + avg);
				}
			}
		}
	}
}
