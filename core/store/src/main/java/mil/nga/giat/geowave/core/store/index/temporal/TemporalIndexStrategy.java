package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class TemporalIndexStrategy implements
		FieldIndexStrategy<TemporalQueryConstraint, Date>
{
	private static final String ID = "TEMPORAL";

	public TemporalIndexStrategy() {
		super();
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
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(indexedRange);
	}

	@Override
	public InsertionIds getInsertionIds(
			final Date indexedData ) {
		return new InsertionIds(
				Collections.singletonList(new ByteArrayId(
						toIndexByte(indexedData))));
	}

	@Override
	public InsertionIds getInsertionIds(
			final Date indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public Date getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return new Date(
				Lexicoders.LONG.fromByteArray(sortKey.getBytes()));
	}

}
