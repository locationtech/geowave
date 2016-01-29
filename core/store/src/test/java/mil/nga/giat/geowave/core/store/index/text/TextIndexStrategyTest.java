package mil.nga.giat.geowave.core.store.index.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Test;

public class TextIndexStrategyTest
{
	private final ByteArrayId fieldId = new ByteArrayId(
			"filler");

	@Test
	public void testInsertions() {
		final TextIndexStrategy strategy = new TextIndexStrategy();
		final FieldInfo<String> fieldInfo = new FieldInfo<>(
				new PersistentValue<String>(
						null,
						"inability to deal with or understand something complicated or unaccountable"),
				null,
				null);
		final List<FieldInfo<String>> fieldInfoList = new ArrayList<>();
		fieldInfoList.add(fieldInfo);
		final List<ByteArrayId> ids = strategy.getInsertionIds(fieldInfoList);
		assertTrue(ids.contains(new ByteArrayId(
				"\01i")));
		assertTrue(ids.contains(new ByteArrayId(
				"ity ")));
		assertTrue(ids.contains(new ByteArrayId(
				"le\01")));
		assertEquals(
				225,
				ids.size());
	}

	@Test
	public void testQueryTextRange() {
		final TextIndexStrategy strategy = new TextIndexStrategy(
				3,
				4);
		List<ByteArrayRange> ranges = strategy.getQueryRanges(new FilterableTextRangeConstraint(
				fieldId,
				"deal",
				"dumn",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dum")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new FilterableTextRangeConstraint(
				fieldId,
				"dealing",
				"durango",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dur")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new FilterableTextRangeConstraint(
				fieldId,
				"d",
				"e",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\00"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01e\177")))),
				ranges.get(0));

	}

	@Test
	public void testQueryTestLike() {
		final TextIndexStrategy strategy = new TextIndexStrategy(
				3,
				4);
		List<ByteArrayRange> ranges = strategy.getQueryRanges(new FilterableLikeConstraint(
				fieldId,
				"deal%",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new FilterableLikeConstraint(
				fieldId,
				"de%",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01de"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01de")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new FilterableLikeConstraint(
				fieldId,
				"d%aling",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\00"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\177")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new FilterableTextRangeConstraint(
				fieldId,
				"d",
				"e",
				false));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\00"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01e\177")))),
				ranges.get(0));

	}
}
