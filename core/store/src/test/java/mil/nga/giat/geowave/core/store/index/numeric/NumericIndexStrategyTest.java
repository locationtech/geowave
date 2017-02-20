package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;

public class NumericIndexStrategyTest
{
	private final NumericFieldIndexStrategy strategy = new NumericFieldIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final int number = 10;

	@Test
	public void testInsertions() {
		final InsertionIds insertionIds = strategy.getInsertionIds(number);
		final List<ByteArrayId> compositieInsertionIds = insertionIds.getCompositeInsertionIds();
		Assert.assertTrue(compositieInsertionIds.contains(new ByteArrayId(
				Lexicoders.DOUBLE.toByteArray((double) number))));
		Assert.assertTrue(compositieInsertionIds.size() == 1);
	}

	@Test
	public void testEquals() {
		final QueryRanges ranges = strategy.getQueryRanges(new NumericEqualsConstraint(
				fieldId,
				number));
		Assert.assertTrue(!ranges.isMultiRange());
		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)),
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)))));
	}

	@Test
	public void testGreaterThanOrEqualTo() {
		final QueryRanges ranges = strategy.getQueryRanges(new NumericGreaterThanOrEqualToConstraint(
				fieldId,
				number));
		Assert.assertTrue(!ranges.isMultiRange());
		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)),
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) Lexicoders.DOUBLE.getMaximumValue())))));
	}

	@Test
	public void testLessThanOrEqualTo() {
		final NumericFieldIndexStrategy strategy = new NumericFieldIndexStrategy();
		final QueryRanges ranges = strategy.getQueryRanges(new NumericLessThanOrEqualToConstraint(
				fieldId,
				number));
		Assert.assertTrue(!ranges.isMultiRange());

		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) Lexicoders.DOUBLE.getMinimumValue())),
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)))));
	}
}
