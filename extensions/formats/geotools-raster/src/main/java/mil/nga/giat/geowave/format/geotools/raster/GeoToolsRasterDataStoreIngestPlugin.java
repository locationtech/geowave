package mil.nga.giat.geowave.format.geotools.raster;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.log4j.Logger;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.opengis.coverage.grid.GridCoverage;

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
	private final static Logger LOGGER = Logger.getLogger(GeoToolsRasterDataStoreIngestPlugin.class);
	private final Index[] supportedIndices;
	private final RasterOptionProvider optionProvider;

	public GeoToolsRasterDataStoreIngestPlugin() {
		this(
				new RasterOptionProvider());
	}

	public GeoToolsRasterDataStoreIngestPlugin(
			final RasterOptionProvider optionProvider ) {
		supportedIndices = new Index[] {
			IndexType.SPATIAL_RASTER.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL_RASTER.createDefaultIndex()
		};
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

		final AbstractGridFormat format = GridFormatFinder.findFormat(file);
		// the null check is enough and we don't need to check the format
		// accepts this file because the finder should have previously validated
		// this
		return (format != null);
	}

	@Override
	public CloseableIterator<GeoWaveData<GridCoverage>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {

		final AbstractGridFormat format = GridFormatFinder.findFormat(input);
		final GridCoverage2DReader reader = format.getReader(input);
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
				final RasterDataAdapter adapter = new RasterDataAdapter(
						input.getName(),
						metadata,
						coverage,
						optionProvider.getTileSize(),
						optionProvider.isBuildPyramid());
				final List<GeoWaveData<GridCoverage>> coverages = new ArrayList<GeoWaveData<GridCoverage>>();
				coverages.add(new GeoWaveData<GridCoverage>(
						adapter,
						primaryIndexId,
						coverage));
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
				LOGGER.warn("Null grid coverage from file '" + input.getAbsolutePath() + "' for discovered geotools format '" + format.getName() + "'");
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read grid coverage of file '" + input.getAbsolutePath() + "' for discovered geotools format '" + format.getName() + "'",
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
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

}
