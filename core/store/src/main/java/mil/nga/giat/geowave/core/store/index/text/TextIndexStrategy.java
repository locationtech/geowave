package mil.nga.giat.geowave.core.store.index.text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
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
	public List<ByteArrayRange> getQueryRanges(
			final TextQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getRange();
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TextQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<String>> indexedData ) {
		final List<ByteArrayId> insertionIds = new ArrayList<>();
		for (FieldInfo<String> fieldInfo : indexedData) {
			insertionIds.add(new ByteArrayId(
					fieldInfo.getDataValue().getValue()));
		}
		return insertionIds;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<String>> indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<String>> getRangeForId(
			final ByteArrayId insertionId ) {
		return Collections.emptyList();
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		return null;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}
}
