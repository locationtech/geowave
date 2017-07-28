/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.landsat8;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.io.LineReader;
import com.vividsolutions.jts.geom.MultiPolygon;

import mil.nga.giat.geowave.core.index.StringUtils;

public class SceneFeatureIterator implements
		SimpleFeatureIterator
{
	protected static class BestCloudCoverComparator implements
			Comparator<SimpleFeature>,
			Serializable
	{
		private static final long serialVersionUID = -5294130929073387335L;

		@Override
		public int compare(
				final SimpleFeature first,
				final SimpleFeature second ) {
			return Float.compare(
					(Float) first.getAttribute(CLOUD_COVER_ATTRIBUTE_NAME),
					(Float) second.getAttribute(CLOUD_COVER_ATTRIBUTE_NAME));
		}
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(SceneFeatureIterator.class);
	private static final String SCENES_GZ_URL = "http://landsat-pds.s3.amazonaws.com/scene_list.gz";
	protected static final String SCENES_TYPE_NAME = "scene";
	public static final String SHAPE_ATTRIBUTE_NAME = "shape";
	public static final String ACQUISITION_DATE_ATTRIBUTE_NAME = "acquisitionDate";
	public static final String CLOUD_COVER_ATTRIBUTE_NAME = "cloudCover";
	public static final String PROCESSING_LEVEL_ATTRIBUTE_NAME = "processingLevel";
	public static final String PATH_ATTRIBUTE_NAME = "path";
	public static final String ROW_ATTRIBUTE_NAME = "row";
	public static final String SCENE_DOWNLOAD_ATTRIBUTE_NAME = "sceneDownloadUrl";
	public static final String ENTITY_ID_ATTRIBUTE_NAME = "entityId";

	protected static final String[] SCENE_ATTRIBUTES = new String[] {
		SHAPE_ATTRIBUTE_NAME,
		ACQUISITION_DATE_ATTRIBUTE_NAME,
		CLOUD_COVER_ATTRIBUTE_NAME,
		PROCESSING_LEVEL_ATTRIBUTE_NAME,
		PATH_ATTRIBUTE_NAME,
		ROW_ATTRIBUTE_NAME,
		ENTITY_ID_ATTRIBUTE_NAME,
		SCENE_DOWNLOAD_ATTRIBUTE_NAME
	};
	protected static String AQUISITION_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	private final String SCENES_DIR = "scenes";
	private final String COMPRESSED_FILE_NAME = "scene_list.gz";
	private final String CSV_FILE_NAME = "scene_list";
	private final String TEMP_CSV_FILE_NAME = "scene_list.tmp";
	private CSVParser parser;
	private FileInputStream parserFis;
	private InputStreamReader parserIsr;
	private Iterator<SimpleFeature> iterator;
	private SimpleFeatureType type;

	public SceneFeatureIterator(
			final boolean onlyScenesSinceLastRun,
			final boolean useCachedScenes,
			final boolean nBestScenesByPathRow,
			final int nBestScenes,
			final Filter cqlFilter,
			final String workspaceDir )
			throws MalformedURLException,
			IOException {
		init(
				new File(
						workspaceDir,
						SCENES_DIR),
				onlyScenesSinceLastRun,
				useCachedScenes,
				nBestScenesByPathRow,
				nBestScenes,
				new WRS2GeometryStore(
						workspaceDir),
				cqlFilter);
	}

	private void init(
			final File scenesDir,
			final boolean onlyScenesSinceLastRun,
			final boolean useCachedScenes,
			final boolean nBestScenesByPathRow,
			final int nBestScenes,
			final WRS2GeometryStore geometryStore,
			final Filter cqlFilter )
			throws IOException {
		if (!scenesDir.exists() && !scenesDir.mkdirs()) {
			LOGGER.warn("Unable to create directory '" + scenesDir.getAbsolutePath() + "'");
		}
		final File csvFile = new File(
				scenesDir,
				CSV_FILE_NAME);
		long startLine = 0;
		if (!csvFile.exists() || !useCachedScenes) {
			final File compressedFile = new File(
					scenesDir,
					COMPRESSED_FILE_NAME);
			final File tempCsvFile = new File(
					scenesDir,
					TEMP_CSV_FILE_NAME);
			if (compressedFile.exists()) {
				if (!compressedFile.delete()) {
					LOGGER.warn("Unable to delete '" + compressedFile.getAbsolutePath() + "'");
				}
			}
			if (tempCsvFile.exists()) {
				if (!tempCsvFile.delete()) {
					LOGGER.warn("Unable to delete '" + tempCsvFile.getAbsolutePath() + "'");
				}
			}
			InputStream in = null;
			// first download the gzipped file
			final FileOutputStream outStream = new FileOutputStream(
					compressedFile);
			try {
				in = new URL(
						SCENES_GZ_URL).openStream();
				IOUtils.copyLarge(
						in,
						outStream);
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read scenes from public S3",
						e);
				throw e;
			}
			finally {
				if (outStream != null) {
					outStream.close();
				}

				if (in != null) {
					IOUtils.closeQuietly(in);
				}
			}
			// next unzip to CSV
			GzipCompressorInputStream gzIn = null;
			FileOutputStream out = null;
			FileInputStream fin = null;
			BufferedInputStream bin = null;
			try {
				fin = new FileInputStream(
						compressedFile);
				bin = new BufferedInputStream(
						fin);
				out = new FileOutputStream(
						tempCsvFile);
				gzIn = new GzipCompressorInputStream(
						bin);
				final byte[] buffer = new byte[1024];
				int n = 0;
				while (-1 != (n = gzIn.read(buffer))) {
					out.write(
							buffer,
							0,
							n);
				}
				fin.close();
				// once we have a csv we can cleanup the compressed file
				if (!compressedFile.delete()) {
					LOGGER.warn("Unable to delete '" + compressedFile.getAbsolutePath() + "'");
				}
				out.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to extract scenes file",
						e);
				throw e;
			}
			finally {
				// HP Fortify "Unreleased Resource" false positive
				// These streams are closed if not null, in this
				// "finally" block
				if (out != null) {
					IOUtils.closeQuietly(out);
				}
				if (gzIn != null) {
					IOUtils.closeQuietly(gzIn);
				}
				if (fin != null) {
					IOUtils.closeQuietly(fin);
				}
				if (bin != null) {
					IOUtils.closeQuietly(bin);
				}
			}
			if (onlyScenesSinceLastRun && csvFile.exists()) {
				// seek the number of lines of the existing file
				try (final FileInputStream is = new FileInputStream(
						csvFile)) {
					final LineReader lines = new LineReader(
							new InputStreamReader(
									is,
									StringUtils.UTF8_CHAR_SET));
					while (lines.readLine() != null) {
						startLine++;
					}
				}
			}
			if (csvFile.exists()) {
				if (!csvFile.delete()) {
					LOGGER.warn("Unable to delete '" + csvFile.getAbsolutePath() + "'");
				}
			}
			if (!tempCsvFile.renameTo(csvFile)) {
				LOGGER.warn("Unable to rename '" + tempCsvFile.getAbsolutePath() + "' to '" + csvFile.getAbsolutePath()
						+ "'");
			}
		}
		type = createFeatureType();
		setupCsvToFeatureIterator(
				csvFile,
				startLine,
				geometryStore,
				cqlFilter);
		if (nBestScenes > 0) {
			nBestScenes(
					nBestScenesByPathRow,
					nBestScenes);
		}
	}

	public static SimpleFeatureType createFeatureType() {
		// initialize the feature type
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(SCENES_TYPE_NAME);
		typeBuilder.add(
				SHAPE_ATTRIBUTE_NAME,
				MultiPolygon.class);
		typeBuilder.add(
				ENTITY_ID_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				ACQUISITION_DATE_ATTRIBUTE_NAME,
				Date.class);
		typeBuilder.add(
				CLOUD_COVER_ATTRIBUTE_NAME,
				Float.class);
		typeBuilder.add(
				PROCESSING_LEVEL_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				PATH_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				ROW_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				SCENE_DOWNLOAD_ATTRIBUTE_NAME,
				String.class);
		return typeBuilder.buildFeatureType();
	}

	private boolean hasOtherProperties(
			final Filter cqlFilter ) {
		final String[] attributes = DataUtilities.attributeNames(
				cqlFilter,
				type);
		for (final String attr : attributes) {
			if (!ArrayUtils.contains(
					SCENE_ATTRIBUTES,
					attr)) {
				return true;
			}
		}
		return false;
	}

	private void nBestScenes(
			final boolean byPathRow,
			final int n ) {
		iterator = nBestScenes(
				this,
				byPathRow,
				n);
	}

	private static class PathRowPair
	{
		private final int path;
		private final int row;

		public PathRowPair(
				final int path,
				final int row ) {
			this.path = path;
			this.row = row;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + path;
			result = (prime * result) + row;
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final PathRowPair other = (PathRowPair) obj;
			if (path != other.path) {
				return false;
			}
			if (row != other.row) {
				return false;
			}
			return true;
		}
	}

	protected static Iterator<SimpleFeature> nBestScenes(
			final SimpleFeatureIterator iterator,
			final boolean byPathRow,
			final int n ) {
		if (byPathRow) {
			final Map<PathRowPair, MinMaxPriorityQueue<SimpleFeature>> bestScenes = new HashMap<>();
			while (iterator.hasNext()) {
				final SimpleFeature feature = iterator.next();
				final Integer path = (Integer) feature.getAttribute(PATH_ATTRIBUTE_NAME);
				final Integer row = (Integer) feature.getAttribute(ROW_ATTRIBUTE_NAME);
				final PathRowPair pr = new PathRowPair(
						path,
						row);
				MinMaxPriorityQueue<SimpleFeature> queue = bestScenes.get(pr);
				if (queue == null) {
					queue = MinMaxPriorityQueue.orderedBy(
							new BestCloudCoverComparator()).maximumSize(
							n).create();
					bestScenes.put(
							pr,
							queue);
				}
				queue.offer(feature);
			}
			final List<Iterator<SimpleFeature>> iterators = new ArrayList<Iterator<SimpleFeature>>();
			for (final MinMaxPriorityQueue<SimpleFeature> queue : bestScenes.values()) {
				iterators.add(queue.iterator());
			}
			return Iterators.concat(iterators.iterator());
		}

		final MinMaxPriorityQueue<SimpleFeature> bestScenes = MinMaxPriorityQueue.orderedBy(
				new BestCloudCoverComparator()).maximumSize(
				n).create();
		// iterate once through the scenes, saving the best entity IDs
		// based on cloud cover

		while (iterator.hasNext()) {
			bestScenes.offer(iterator.next());
		}
		iterator.close();
		return bestScenes.iterator();
	}

	private void setupCsvToFeatureIterator(
			final File csvFile,
			final long startLine,
			final WRS2GeometryStore geometryStore,
			final Filter cqlFilter )
			throws FileNotFoundException,
			IOException {

		parserFis = new FileInputStream(
				csvFile);
		parserIsr = new InputStreamReader(
				parserFis,
				StringUtils.UTF8_CHAR_SET);
		parser = new CSVParser(
				parserIsr,
				CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord());
		final Iterator<CSVRecord> csvIterator = parser.iterator();
		long startLineDecrementor = startLine;
		// we skip the header, so only skip to start line 1
		while ((startLineDecrementor > 1) && csvIterator.hasNext()) {
			startLineDecrementor--;
			csvIterator.next();
		}

		// wrap the iterator with a feature conversion and a filter (if
		// provided)
		iterator = Iterators.transform(
				csvIterator,
				new CSVToFeatureTransform(
						geometryStore,
						type));
		if (cqlFilter != null) {
			Filter actualFilter;
			if (hasOtherProperties(cqlFilter)) {
				final PropertyIgnoringFilterVisitor visitor = new PropertyIgnoringFilterVisitor(
						SCENE_ATTRIBUTES,
						type);
				actualFilter = (Filter) cqlFilter.accept(
						visitor,
						null);
			}
			else {
				actualFilter = cqlFilter;
			}
			final CqlFilterPredicate filterPredicate = new CqlFilterPredicate(
					actualFilter);
			iterator = Iterators.filter(
					iterator,
					filterPredicate);
		}
	}

	public SimpleFeatureType getFeatureType() {
		return type;
	}

	@Override
	public void close() {
		if (parser != null) {
			try {
				parser.close();
				parser = null;
				parserFis.close();
				parserFis = null;
				parserIsr.close();
				parserIsr = null;
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close CSV parser",
						parser,
						e);
			}
		}
	}

	@Override
	public boolean hasNext() {
		if (iterator != null) {
			return iterator.hasNext();
		}
		return false;
	}

	@Override
	public SimpleFeature next()
			throws NoSuchElementException {
		if (iterator != null) {
			return iterator.next();
		}
		return null;
	}

	private static class CSVToFeatureTransform implements
			Function<CSVRecord, SimpleFeature>
	{
		// shape (Geometry), entityId (String), acquisitionDate (Date),
		// cloudCover (double), processingLevel (String), path (int), row (int)
		private final WRS2GeometryStore wrs2Geometry;
		private final SimpleFeatureBuilder featureBuilder;

		public CSVToFeatureTransform(
				final WRS2GeometryStore wrs2Geometry,
				final SimpleFeatureType type ) {
			this.wrs2Geometry = wrs2Geometry;

			featureBuilder = new SimpleFeatureBuilder(
					type);
		}

		// entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon,max_lat,max_lon,download_url
		@Override
		public SimpleFeature apply(
				final CSVRecord input ) {
			final String entityId = input.get("entityId");
			final double cloudCover = Double.parseDouble(input.get("cloudCover"));
			final String processingLevel = input.get("processingLevel");
			final int path = Integer.parseInt(input.get("path"));
			final int row = Integer.parseInt(input.get("row"));
			final String downloadUrl = input.get("download_url");

			final MultiPolygon shape = wrs2Geometry.getGeometry(
					path,
					row);
			featureBuilder.add(shape);
			featureBuilder.add(entityId);
			Date aquisitionDate;
			final SimpleDateFormat sdf = new SimpleDateFormat(
					AQUISITION_DATE_FORMAT);
			try {
				aquisitionDate = sdf.parse(input.get("acquisitionDate"));
				featureBuilder.add(aquisitionDate);
			}
			catch (final ParseException e) {
				LOGGER.warn(
						"Unable to parse aquisition date",
						e);

				featureBuilder.add(null);
			}

			featureBuilder.add(cloudCover);
			featureBuilder.add(processingLevel);
			featureBuilder.add(path);
			featureBuilder.add(row);
			featureBuilder.add(downloadUrl);
			return featureBuilder.buildFeature(entityId);
		}
	}

	private static class CqlFilterPredicate implements
			Predicate<SimpleFeature>
	{
		private final Filter cqlFilter;

		public CqlFilterPredicate(
				final Filter cqlFilter ) {
			this.cqlFilter = cqlFilter;
		}

		@Override
		public boolean apply(
				final SimpleFeature input ) {
			return cqlFilter.evaluate(input);
		}

	}
}
