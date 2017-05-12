package mil.nga.giat.geowave.format.geotools.raster;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This plugin is used for ingesting any GeoTools supported file data store from
 * a local file system directly into GeoWave as GeoTools' SimpleFeatures. It
 * supports the default configuration of spatial and spatial-temporal indices
 * and does NOT currently support the capability to stage intermediate data to
 * HDFS to be ingested using a map-reduce job.
 */
public class GeoToolsRasterDataStoreIngestPlugin implements
		LocalFileIngestPlugin<GridCoverage>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoToolsRasterDataStoreIngestPlugin.class);
	private final RasterOptionProvider optionProvider;

	public GeoToolsRasterDataStoreIngestPlugin() {
		this(
				new RasterOptionProvider());
	}

	public GeoToolsRasterDataStoreIngestPlugin(
			final RasterOptionProvider optionProvider ) {
		this.optionProvider = optionProvider;
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {};
	}

	@Override
	public void init(
			final File baseDirectory ) {}

	@Override
	public boolean supportsFile(
			final File file ) {
		AbstractGridFormat format = null;
		try {
			format = GridFormatFinder.findFormat(file);
		}
		catch (final Exception e) {
			LOGGER.info(
					"Unable to support as raster file",
					e);
		}
		// the null check is enough and we don't need to check the format
		// accepts this file because the finder should have previously validated
		// this
		return (format != null);
	}

	private static AbstractGridFormat prioritizedFindFormat(
			final File input ) {
		final AbstractGridFormat format = null;
		try {
			final Set<AbstractGridFormat> formats = GridFormatFinder.findFormats(input);
			if ((formats == null) || formats.isEmpty()) {
				LOGGER.warn("Unable to support raster file " + input.getAbsolutePath());
				return null;
			}
			// world image and geotiff can both open tif files, give
			// priority to gdalgeotiff, followed by geotiff
			for (final AbstractGridFormat f : formats) {
				if ("GDALGeoTiff".equals(f.getName())) {
					return f;
				}
			}
			for (final AbstractGridFormat f : formats) {
				if ("GeoTIFF".equals(f.getName())) {
					return f;
				}
			}

			// otherwise just pick the first
			final Iterator<AbstractGridFormat> it = formats.iterator();
			if (it.hasNext()) {
				return it.next();
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error while trying read raster file",
					e);
			return null;
		}
		return format;
	}

	@Override
	public CloseableIterator<GeoWaveData<GridCoverage>> toGeoWaveData(
			final File input,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		final AbstractGridFormat format = prioritizedFindFormat(input);
		if (format == null) {
			return new Wrapper(
					Collections.emptyIterator());
		}
		Hints hints = null;
		if ((optionProvider.getCrs() != null) && !optionProvider.getCrs().trim().isEmpty()) {
			try {
				final CoordinateReferenceSystem crs = CRS.decode(optionProvider.getCrs());
				if (crs != null) {
					hints = new Hints();
					hints.put(
							Hints.DEFAULT_COORDINATE_REFERENCE_SYSTEM,
							crs);
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to find coordinate reference system, continuing without hint",
						e);
			}
		}
		final GridCoverage2DReader reader = format.getReader(
				input,
				hints);
		if (reader == null) {
			LOGGER.error("Unable to get reader instance, getReader returned null");
			return new Wrapper(
					Collections.emptyIterator());
		}
		try {
			final GridCoverage2D coverage = reader.read(null);
			if (coverage != null) {
				final Map<String, String> metadata = new HashMap<String, String>();
				final String coverageName = coverage.getName().toString();
				try {
					// wrapping with try-catch block because often the reader
					// does not support operations on coverage name
					// if not, we just don't have metadata, and continue
					final String[] mdNames = reader.getMetadataNames(coverageName);
					if ((mdNames != null) && (mdNames.length > 0)) {
						for (final String mdName : mdNames) {
							metadata.put(
									mdName,
									reader.getMetadataValue(
											coverageName,
											mdName));
						}
					}
				}
				catch (final Exception e) {
					LOGGER.debug(
							"Unable to find metadata from coverage reader",
							e);
				}
				final List<GeoWaveData<GridCoverage>> coverages = new ArrayList<GeoWaveData<GridCoverage>>();

				if (optionProvider.isSeparateBands() && (coverage.getNumSampleDimensions() > 1)) {
					final String baseName = optionProvider.getCoverageName() != null ? optionProvider.getCoverageName()
							: input.getName();
					final double[][] nodata = optionProvider.getNodata(coverage.getNumSampleDimensions());
					for (int b = 0; b < coverage.getNumSampleDimensions(); b++) {
						final RasterDataAdapter adapter = new RasterDataAdapter(
								baseName + "_B" + b,
								metadata,
								(GridCoverage2D) RasterUtils.getCoverageOperations().selectSampleDimension(
										coverage,
										new int[] {
											b
										}),
								optionProvider.getTileSize(),
								optionProvider.isBuildPyramid(),
								optionProvider.isBuildHistogream(),
								new double[][] {
									nodata[b]
								});
						coverages.add(new GeoWaveData<GridCoverage>(
								adapter,
								primaryIndexIds,
								coverage));
					}
				}
				else {
					final RasterDataAdapter adapter = new RasterDataAdapter(
							optionProvider.getCoverageName() != null ? optionProvider.getCoverageName()
									: input.getName(),
							metadata,
							coverage,
							optionProvider.getTileSize(),
							optionProvider.isBuildPyramid(),
							optionProvider.isBuildHistogream(),
							optionProvider.getNodata(coverage.getNumSampleDimensions()));
					coverages.add(new GeoWaveData<GridCoverage>(
							adapter,
							primaryIndexIds,
							coverage));
				}
				return new Wrapper(
						coverages.iterator()) {

					@Override
					public void close()
							throws IOException {
						reader.dispose();
					}
				};
			}
			else {
				LOGGER.warn("Null grid coverage from file '" + input.getAbsolutePath()
						+ "' for discovered geotools format '" + format.getName() + "'");
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read grid coverage of file '" + input.getAbsolutePath()
							+ "' for discovered geotools format '" + format.getName() + "'",
					e);
		}
		return new Wrapper(
				Collections.emptyIterator());
	}

	@Override
	public WritableDataAdapter<GridCoverage>[] getDataAdapters(
			final String globalVisibility ) {
		return new WritableDataAdapter[] {};
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class
		};
	}
}
