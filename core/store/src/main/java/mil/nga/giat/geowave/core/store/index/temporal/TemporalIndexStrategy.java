package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class TemporalIndexStrategy implements
		FieldIndexStrategy<TemporalQueryConstraint, Date>
{
	private static final String ID = "TEMPORAL";

	public TemporalIndexStrategy() {
		super();
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TemporalQueryConstraint indexedRange ) {
		return indexedRange.getRange();
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition ) {
		return getQueryRanges(indexedRange);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<Date>> indexedData ) {
		final List<ByteArrayId> insertionIds = new ArrayList<>();
		for (final FieldInfo<Date> fieldInfo : indexedData) {
			insertionIds.add(new ByteArrayId(
					toIndexByte(fieldInfo.getDataValue().getValue())));
		}
		return insertionIds;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<Date>> indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<Date>> getRangeForId(
			final ByteArrayId insertionId ) {
		return Collections.emptyList();
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	public static final byte[] toIndexByte(
			final Date date ) {
		return Lexicoders.LONG.toByteArray(date.getTime());
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		return null;
	}

}
