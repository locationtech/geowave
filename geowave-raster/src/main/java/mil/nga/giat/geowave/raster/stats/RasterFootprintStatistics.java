package mil.nga.giat.geowave.raster.stats;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.raster.RasterUtils;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.adapter.statistics.AbstractDataStatistics;

import org.apache.log4j.Logger;
import org.opengis.coverage.grid.GridCoverage;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

public class RasterFootprintStatistics extends
		AbstractDataStatistics<GridCoverage>
{
	private static final Logger LOGGER = Logger.getLogger(RasterFootprintStatistics.class);
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"FOOTPRINT");
	private Geometry footprint;

	protected RasterFootprintStatistics() {
		super();
	}

	public RasterFootprintStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId,
				STATS_ID);
	}

	@Override
	public byte[] toBinary() {
		if (footprint == null) {
			return new byte[] {};
		}
		else {
			return new WKBWriter().write(footprint);
		}
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length > 0) {
			try {
				footprint = new WKBReader().read(bytes);
			}
			catch (final ParseException e) {
				LOGGER.warn(
						"Unable to parse WKB",
						e);
			}
		}
		else {
			footprint = null;
		}
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final GridCoverage entry ) {
		if (entry instanceof FitToIndexGridCoverage) {
			footprint = RasterUtils.combineIntoOneGeometry(
					footprint,
					((FitToIndexGridCoverage) entry).getFootprintWorldGeometry());
		}
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if (statistics instanceof RasterFootprintStatistics) {
			footprint = RasterUtils.combineIntoOneGeometry(
					footprint,
					((RasterFootprintStatistics) statistics).footprint);
		}
	}
}
