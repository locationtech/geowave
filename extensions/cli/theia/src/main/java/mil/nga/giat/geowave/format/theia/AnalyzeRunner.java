/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.format.theia.AnalyzeRunner;
import mil.nga.giat.geowave.format.theia.BandFeatureIterator;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.SceneFeatureIterator;

public class AnalyzeRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AnalyzeRunner.class);

	protected TheiaBasicCommandLineOptions theiaOptions = new TheiaBasicCommandLineOptions();

	public AnalyzeRunner(
			final TheiaBasicCommandLineOptions theiaOptions ) {
		this.theiaOptions = theiaOptions;
	}

	protected void runInternal(
			final OperationParams params )
			throws Exception {
		try {
			try (BandFeatureIterator bands = new BandFeatureIterator(
					theiaOptions.collection(),
					theiaOptions.platform(),
					theiaOptions.location(),
					theiaOptions.startDate(),
					theiaOptions.endDate(),
					theiaOptions.orbitNumber(),
					theiaOptions.relativeOrbitNumber(),
					theiaOptions.getCqlFilter(),
					theiaOptions.getWorkspaceDir())) {
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
		private final TreeMap<String, SimpleFeature> entityBandIdToSimpleFeatureMap = new TreeMap<String, SimpleFeature>();
		private int sceneCount = 0;
		private double minLat = Double.MAX_VALUE;
		private double minLon = Double.MAX_VALUE;
		private double maxLat = -Double.MAX_VALUE;
		private double maxLon = -Double.MAX_VALUE;
		private long startDate = Long.MAX_VALUE;
		private long endDate = 0;
		private float totalCloudCover = 0f;
		private int minCloudCover = Integer.MAX_VALUE;
		private int maxCloudCover = -Integer.MAX_VALUE;
		private final Map<String, Integer> processingLevelCounts = new HashMap<String, Integer>();

		private void nextScene(
				final SimpleFeature currentBand ) {
			printSceneInfo();
			sceneCount++;
			entityBandIdToSimpleFeatureMap.clear();

			final Envelope env = ((Geometry) currentBand.getDefaultGeometry()).getEnvelopeInternal();
			final Date date = (Date) currentBand.getAttribute(SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME);
			final String processingLevel = (String) currentBand
					.getAttribute(SceneFeatureIterator.PROCESSING_LEVEL_ATTRIBUTE_NAME);
			final int cloudCover = (int) currentBand.getAttribute(SceneFeatureIterator.CLOUD_COVER_ATTRIBUTE_NAME);

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

			startDate = Math.min(
					startDate,
					date.getTime());
			endDate = Math.max(
					endDate,
					date.getTime());

			Integer count = processingLevelCounts.get(processingLevel);
			if (count == null) {
				count = 0;
			}
			count++;
			processingLevelCounts.put(
					processingLevel,
					count);

			minCloudCover = Math.min(
					minCloudCover,
					cloudCover);
			maxCloudCover = Math.max(
					maxCloudCover,
					cloudCover);
			totalCloudCover += cloudCover;
		}

		private void addBandInfo(
				final SimpleFeature band ) {
			String bandName = (String) band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME);
			entityBandIdToSimpleFeatureMap.put(
					bandName,
					band);
		}

		private void printSceneInfo() {
			if (sceneCount > 0) {
				final SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss.SSS");

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
								+ feature.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME) + "   -->");
						System.out
								.println("Acquisition Date: "
										+ sdf.format(feature
												.getAttribute(SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME)));
						System.out.println("Location: "
								+ feature.getAttribute(SceneFeatureIterator.LOCATION_ATTRIBUTE_NAME));
						System.out.println("Product Identifier: "
								+ feature.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME));
						System.out.println("Product Type: "
								+ feature.getAttribute(SceneFeatureIterator.PRODUCT_TYPE_ATTRIBUTE_NAME));
						System.out.println("Collection: "
								+ feature.getAttribute(SceneFeatureIterator.COLLECTION_ATTRIBUTE_NAME));
						System.out.println("Platform: "
								+ feature.getAttribute(SceneFeatureIterator.PLATFORM_ATTRIBUTE_NAME));
						System.out.println("Processing Level: "
								+ feature.getAttribute(SceneFeatureIterator.PROCESSING_LEVEL_ATTRIBUTE_NAME));
						System.out.println("Quicklook: "
								+ feature.getAttribute(SceneFeatureIterator.QUICKLOOK_ATTRIBUTE_NAME));
						System.out.println("Thumbnail: "
								+ feature.getAttribute(SceneFeatureIterator.THUMBNAIL_ATTRIBUTE_NAME));
						System.out.println("Cloud Cover: "
								+ feature.getAttribute(SceneFeatureIterator.CLOUD_COVER_ATTRIBUTE_NAME));
						System.out.println("Snow Cover: "
								+ feature.getAttribute(SceneFeatureIterator.SNOW_COVER_ATTRIBUTE_NAME));
						System.out.println("Water Cover: "
								+ feature.getAttribute(SceneFeatureIterator.WATER_COVER_ATTRIBUTE_NAME));
						System.out.println("Orbit Number: "
								+ feature.getAttribute(SceneFeatureIterator.ORBIT_NUMBER_ATTRIBUTE_NAME));
						System.out.println("Relative Orbit Number: "
								+ feature.getAttribute(SceneFeatureIterator.RELATIVE_ORBIT_NUMBER_ATTRIBUTE_NAME));
						first = false;
					}
					// print band info
					System.out.println("Band " + bandId);
				}
			}
		}

		private void printTotals() {
			System.out.println("\n<--   Totals   -->");
			System.out.println("Total Scenes: " + sceneCount);

			if (sceneCount > 0) {
				final SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss.SSS");

				System.out.println("Date Range: [" + sdf.format(new Date(
						startDate)) + ", " + sdf.format(new Date(
						endDate)) + "]");
				System.out.println("Cloud Cover Range: [" + minCloudCover + ", " + maxCloudCover + "]");
				System.out.println("Average Cloud Cover: " + (totalCloudCover / sceneCount));
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
				System.out.println(strBuf.toString());
			}
		}
	}
}
