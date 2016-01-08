package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Test;

public class TemporalIndexStrategyTest
{
	private final TemporalIndexStrategy strategy = new TemporalIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final Date date = new Date(
			1440080038544L);

	@Test
	public void testInsertions() {
		final List<FieldInfo<Date>> fieldInfoList = new ArrayList<>();
		final FieldInfo<Date> fieldInfo = new FieldInfo<>(
				new PersistentValue<Date>(
						null,
						date),
				null,
				null);
		fieldInfoList.add(fieldInfo);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(fieldInfoList);
		Assert.assertTrue(insertionIds.size() == 1);
		Assert.assertTrue(insertionIds.contains(new ByteArrayId(
				Lexicoders.LONG.toByteArray(date.getTime()))));
	}

	@Test
	public void testDateRange() {
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(new TemporalQueryConstraint(
				fieldId,
				date,
				date));
		Assert.assertTrue(ranges.size() == 1);
		Assert.assertTrue(ranges.get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.LONG.toByteArray(date.getTime())),
						new ByteArrayId(
								Lexicoders.LONG.toByteArray(date.getTime())))));
	}

}
