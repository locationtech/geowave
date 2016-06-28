package mil.nga.giat.geowave.core.store.data.visibility;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.DeleteCallback;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class DifferingFieldVisibilityEntryCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T>
{
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"DIFFERING_VISIBILITY_COUNT");
	private static final ByteArrayId SEPARATOR = new ByteArrayId(
			"_");
	private static final byte[] STATS_ID_AND_SEPARATOR = ArrayUtils.addAll(
			STATS_ID.getBytes(),
			SEPARATOR.getBytes());
	private long entriesWithDifferingFieldVisibilities = 0;

	protected DifferingFieldVisibilityEntryCount() {
		super();
	}

	public long getEntriesWithDifferingFieldVisibilities() {
		return entriesWithDifferingFieldVisibilities;
	}

	public boolean isAnyEntryDifferingFieldVisiblity() {
		return entriesWithDifferingFieldVisibilities > 0;
	}

	private DifferingFieldVisibilityEntryCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statsId,
			final long entriesWithDifferingFieldVisibilities ) {
		super(
				dataAdapterId,
				statsId);
		this.entriesWithDifferingFieldVisibilities = entriesWithDifferingFieldVisibilities;
	}

	public DifferingFieldVisibilityEntryCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId ) {
		super(
				dataAdapterId,
				composeId(indexId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID_AND_SEPARATOR,
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new DifferingFieldVisibilityEntryCount<>(
				dataAdapterId,
				statisticsId,
				entriesWithDifferingFieldVisibilities);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = super.binaryBuffer(8);
		buf.putLong(entriesWithDifferingFieldVisibilities);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		entriesWithDifferingFieldVisibilities = buf.getLong();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (entryHasDifferentVisibilities(entryInfo)) {
			entriesWithDifferingFieldVisibilities++;
		}
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (entryHasDifferentVisibilities(entryInfo)) {
			entriesWithDifferingFieldVisibilities--;
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof DifferingFieldVisibilityEntryCount)) {
			entriesWithDifferingFieldVisibilities += ((DifferingFieldVisibilityEntryCount) merge).entriesWithDifferingFieldVisibilities;
		}
	}

	private static boolean entryHasDifferentVisibilities(
			final DataStoreEntryInfo entryInfo ) {
		if ((entryInfo != null) && (entryInfo.getFieldInfo() != null)) {
			final List<FieldInfo<?>> fields = entryInfo.getFieldInfo();
			// if there is 0 or 1 field, there won't be differing visibilities
			if (fields.size() > 1) {
				final byte[] visibility = fields.get(
						0).getVisibility();
				for (int i = 1; i < fields.size(); i++) {
					if (!Arrays.equals(
							visibility,
							fields.get(
									i).getVisibility())) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public static DifferingFieldVisibilityEntryCount getVisibilityCounts(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		DifferingFieldVisibilityEntryCount combinedVisibilityCount = null;
		for (final ByteArrayId adapterId : adapterIdsToQuery) {
			final DifferingFieldVisibilityEntryCount adapterVisibilityCount = (DifferingFieldVisibilityEntryCount) statisticsStore
					.getDataStatistics(
							adapterId,
							DifferingFieldVisibilityEntryCount.composeId(index.getId()),
							authorizations);
			if (combinedVisibilityCount == null) {
				combinedVisibilityCount = adapterVisibilityCount;
			}
			else {
				combinedVisibilityCount.merge(adapterVisibilityCount);
			}
		}
		return combinedVisibilityCount;
	}
}
