package mil.nga.giat.geowave.adapter.raster.stats;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.coverage.grid.GridCoverage;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class RasterFootprintStatistics extends
		AbstractDataStatistics<GridCoverage>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RasterFootprintStatistics.class);
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FOOTPRINT");
	private Geometry footprint;

	protected RasterFootprintStatistics() {
		super();
	}

	public RasterFootprintStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId,
				STATS_TYPE);
	}

	@Override
	public byte[] toBinary() {
		byte[] bytes = null;
		if (footprint == null) {
			bytes = new byte[] {};
		}
		else {
			bytes = new WKBWriter().write(footprint);
		}
		final ByteBuffer buf = super.binaryBuffer(bytes.length);
		buf.put(bytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		final byte[] payload = buf.array();
		if (payload.length > 0) {
			try {
				footprint = new WKBReader().read(payload);
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
