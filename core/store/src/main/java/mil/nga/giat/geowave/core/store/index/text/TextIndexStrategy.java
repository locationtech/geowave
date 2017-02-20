package mil.nga.giat.geowave.core.store.index.text;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class TextIndexStrategy implements
		FieldIndexStrategy<TextQueryConstraint, String>
{
	private static final String ID = "TEXT";

	public TextIndexStrategy() {
		super();
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TextQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TextQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public InsertionIds getInsertionIds(
			final String indexedData ) {
		return new InsertionIds(
				Collections.singletonList(new ByteArrayId(
						indexedData)));
	}

	@Override
	public InsertionIds getInsertionIds(
			final String indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public String getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return sortKey.getString();
	}
}
