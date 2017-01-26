package mil.nga.giat.geowave.adapter.raster.stats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.opengis.coverage.grid.GridCoverage;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.Resolution;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class OverviewStatistics extends
		AbstractDataStatistics<GridCoverage>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"OVERVIEW");

	private Resolution[] resolutions = new Resolution[] {};

	protected OverviewStatistics() {
		super();
	}

	public OverviewStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId,
				STATS_TYPE);
	}

	@Override
	public byte[] toBinary() {
		synchronized (this) {
			final List<byte[]> resolutionBinaries = new ArrayList<byte[]>(
					resolutions.length);
			int byteCount = 4; // an int for the list size
			for (final Resolution res : resolutions) {
				final byte[] resBinary = PersistenceUtils.toBinary(res);
				resolutionBinaries.add(resBinary);
				byteCount += (resBinary.length + 4); // an int for the binary
														// size
			}

			final ByteBuffer buf = super.binaryBuffer(byteCount);
			buf.putInt(resolutionBinaries.size());
			for (final byte[] resBinary : resolutionBinaries) {
				buf.putInt(resBinary.length);
				buf.put(resBinary);
			}
			return buf.array();
		}
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		final int resLength = buf.getInt();
		synchronized (this) {
			resolutions = new Resolution[resLength];
			for (int i = 0; i < resolutions.length; i++) {
				final byte[] resBytes = new byte[buf.getInt()];
				buf.get(resBytes);
				resolutions[i] = PersistenceUtils.fromBinary(
						resBytes,
						Resolution.class);
			}
		}
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final GridCoverage entry ) {
		if (entry instanceof FitToIndexGridCoverage) {
			final FitToIndexGridCoverage fitEntry = (FitToIndexGridCoverage) entry;
			synchronized (this) {
				resolutions = incorporateResolutions(
						resolutions,
						new Resolution[] {
							fitEntry.getResolution()
						});
			}
		}
	}

	private static Resolution[] incorporateResolutions(
			final Resolution[] res1,
			final Resolution[] res2 ) {
		final TreeSet<Resolution> resolutionSet = new TreeSet<Resolution>();
		for (final Resolution res : res1) {
			resolutionSet.add(res);
		}
		for (final Resolution res : res2) {
			resolutionSet.add(res);
		}
		final Resolution[] combinedRes = new Resolution[resolutionSet.size()];
		int i = 0;
		for (final Resolution res : resolutionSet) {
			combinedRes[i++] = res;
		}
		return combinedRes;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if (statistics instanceof OverviewStatistics) {
			synchronized (this) {
				resolutions = incorporateResolutions(
						resolutions,
						((OverviewStatistics) statistics).getResolutions());
			}
		}
	}

	public Resolution[] getResolutions() {
		synchronized (this) {
			return resolutions;
		}
	}

}
