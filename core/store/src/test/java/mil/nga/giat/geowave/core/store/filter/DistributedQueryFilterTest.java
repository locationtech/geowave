package mil.nga.giat.geowave.core.store.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.BasicQueryFilter.BasicQueryCompareOperation;
import mil.nga.giat.geowave.core.store.query.BasicQueryTest;

import org.junit.Test;

public class DistributedQueryFilterTest
{

	@Test
	public void test() {
		List<DistributableQueryFilter> filters = new ArrayList<DistributableQueryFilter>();
		filters.add(new BasicQueryFilter(
				new BasicNumericDataset(
						new NumericData[] {
							new NumericValue(
									0.4)
						}),
				new NumericDimensionField[] {
					new BasicQueryTest.ExampleDimensionOne()
				},
				BasicQueryCompareOperation.CONTAINS));
		filters.add(new DedupeFilter());
		DistributableFilterList list = new DistributableFilterList(
				false,
				filters);
		list.fromBinary(list.toBinary());
		assertFalse(list.logicalAnd);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).compareOp,
				BasicQueryCompareOperation.CONTAINS);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).constraints,
				new BasicNumericDataset(
						new NumericData[] {
							new NumericRange(
									0.4,
									0.4)
						}));

		filters = new ArrayList<DistributableQueryFilter>();
		filters.add(new BasicQueryFilter(
				new BasicNumericDataset(
						new NumericData[] {
							new NumericValue(
									0.5)
						}),
				new NumericDimensionField[] {
					new BasicQueryTest.ExampleDimensionOne()
				},
				BasicQueryCompareOperation.INTERSECTS));
		filters.add(new DedupeFilter());
		list = new DistributableFilterList(
				true,
				filters);
		list.fromBinary(list.toBinary());
		assertTrue(list.logicalAnd);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).compareOp,
				BasicQueryCompareOperation.INTERSECTS);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).constraints,
				new BasicNumericDataset(
						new NumericData[] {
							new NumericRange(
									0.5,
									0.5)
						}));
	}

}
