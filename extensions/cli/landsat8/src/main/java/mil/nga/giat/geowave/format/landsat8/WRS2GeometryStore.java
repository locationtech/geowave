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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.MultiPolygon;

public class WRS2GeometryStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WRS2GeometryStore.class);

	protected static class WRS2Key
	{
		private final int path;
		private final int row;

		public WRS2Key(
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
			final WRS2Key other = (WRS2Key) obj;
			if (path != other.path) {
				return false;
			}
			if (row != other.row) {
				return false;
			}
			return true;
		}
	}

	private static final String WRS2_TYPE_NAME = "wrs2_asc_desc";
	private static final String WRS2_SHAPE_URL = "https://landsat.usgs.gov/sites/default/files/documents/wrs2_asc_desc.zip";
	private static final String WRS2_SHAPE_NAME = "wrs2_asc_desc.shp";
	private static final String WRS2_SHAPE_ZIP = "wrs2_asc_desc.zip";
	private static final String WRS2_SHAPE_DIRECTORY = "wrs2_asc_desc";
	private final File wrs2Shape;
	private final File wrs2Directory;
	private final Map<WRS2Key, MultiPolygon> featureCache = new HashMap<WRS2Key, MultiPolygon>();
	private SimpleFeatureType wrs2Type;

	public WRS2GeometryStore(
			final String workspaceDirectory )
			throws MalformedURLException,
			IOException {
		wrs2Directory = new File(
				workspaceDirectory,
				WRS2_SHAPE_DIRECTORY);

		wrs2Shape = new File(
				wrs2Directory,
				WRS2_SHAPE_NAME);
		init();
	}

	public SimpleFeatureType getType() {
		return wrs2Type;
	}

	private void init()
			throws MalformedURLException,
			IOException {
		if (!wrs2Shape.exists()) {
			if (!wrs2Directory.delete()) {
				LOGGER.warn("Unable to delete '" + wrs2Directory.getAbsolutePath() + "'");
			}
			final File wsDir = wrs2Directory.getParentFile();
			if (!wsDir.exists() && !wsDir.mkdirs()) {
				LOGGER.warn("Unable to create directory '" + wsDir.getAbsolutePath() + "'");
			}
			// download and unzip the shapefile
			final File targetFile = new File(
					wsDir,
					WRS2_SHAPE_ZIP);
			if (targetFile.exists()) {
				if (!targetFile.delete()) {
					LOGGER.warn("Unable to delete file '" + targetFile.getAbsolutePath() + "'");
				}
			}
			FileUtils.copyURLToFile(
					new URL(
							WRS2_SHAPE_URL),
					targetFile);
			final ZipFile zipFile = new ZipFile(
					targetFile);
			try {
				final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
				while (entries.hasMoreElements()) {
					final ZipArchiveEntry entry = entries.nextElement();
					if (!entry.isDirectory()) {
						FileUtils.copyInputStreamToFile(
								zipFile.getInputStream(entry),
								new File(
										wsDir,
										entry.getName()));
						// HP Fortify "Path Traversal" false positive
						// What Fortify considers "user input" comes only
						// from users with OS-level access anyway
					}
				}
			}
			finally {
				zipFile.close();
			}
		}
		// read the shapefile and cache the features for quick lookup by path
		// and row
		try {
			final Map<String, Object> map = new HashMap<String, Object>();
			map.put(
					"url",
					wrs2Shape.toURI().toURL());
			final DataStore dataStore = DataStoreFinder.getDataStore(map);
			if (dataStore == null) {
				LOGGER.error("Unable to get a datastore instance, getDataStore returned null");
				return;
			}
			final SimpleFeatureSource source = dataStore.getFeatureSource(WRS2_TYPE_NAME);

			final SimpleFeatureCollection featureCollection = source.getFeatures();
			wrs2Type = featureCollection.getSchema();
			final SimpleFeatureIterator iterator = featureCollection.features();
			while (iterator.hasNext()) {
				final SimpleFeature feature = iterator.next();
				final Number path = (Number) feature.getAttribute("PATH");
				final Number row = (Number) feature.getAttribute("ROW");
				featureCache.put(
						new WRS2Key(
								path.intValue(),
								row.intValue()),
						(MultiPolygon) feature.getDefaultGeometry());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to read wrs2_asc_desc shapefile '" + wrs2Shape.getAbsolutePath() + "'",
					e);
			throw (e);
		}
	}

	public MultiPolygon getGeometry(
			final int path,
			final int row ) {
		return featureCache.get(new WRS2Key(
				path,
				row));
	}
}
