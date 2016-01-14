package mil.nga.giat.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.numeric.NumberRangeFilter;
import mil.nga.giat.geowave.core.store.index.numeric.NumericEqualsConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericLessThanConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericQueryConstraint;
import mil.nga.giat.geowave.core.store.index.text.LikeFilter;
import mil.nga.giat.geowave.core.store.index.text.TextQueryConstraint;
import mil.nga.giat.geowave.core.store.index.text.TextRangeFilter;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Test;
import org.opengis.filter.Filter;

public class PropertyFilterVisitorTest
{
	@Test
	public void testNumbersTypes()
			throws CQLException {
		Filter filter = CQL.toFilter("a < 9 and c == 12 and e >= 11 and f <= 12 and g > 13 and h between 4 and 6 and k > 4 and k < 6 and l >= 4 and l <= 6");
		Query query = new Query(
				"type",
				filter);

		PropertyFilterVisitor visitor = new PropertyFilterVisitor();

		PropertyConstraintSet constraints = (PropertyConstraintSet) query.getFilter().accept(
				visitor,
				null);
		NumberRangeFilter nf = (NumberRangeFilter) ((NumericLessThanConstraint) constraints.getConstraintsById(new ByteArrayId(
				"a"))).getFilter();
		assertTrue(nf.getLowerValue().doubleValue() == Double.MIN_VALUE);
		assertEquals(
				9,
				nf.getUpperValue().longValue());
		assertFalse(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"e"))).getFilter();
		assertEquals(
				11,
				nf.getLowerValue().longValue());
		assertTrue(nf.getUpperValue().doubleValue() == Double.MAX_VALUE);
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericEqualsConstraint) constraints.getConstraintsById(new ByteArrayId(
				"c"))).getFilter();
		assertEquals(
				12,
				nf.getLowerValue().longValue());
		assertEquals(
				12,
				nf.getUpperValue().longValue());
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"g"))).getFilter();
		assertEquals(
				13,
				nf.getLowerValue().longValue());
		assertTrue(nf.getUpperValue().doubleValue() == Double.MAX_VALUE);

		assertTrue(nf.isInclusiveHigh());
		assertFalse(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"f"))).getFilter();
		assertEquals(
				12,
				nf.getUpperValue().longValue());
		assertTrue(nf.getLowerValue().doubleValue() == Double.MIN_VALUE);
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"h"))).getFilter();
		assertEquals(
				4,
				nf.getLowerValue().longValue());
		assertEquals(
				6,
				nf.getUpperValue().longValue());
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"k"))).getFilter();
		assertEquals(
				4,
				nf.getLowerValue().longValue());
		assertEquals(
				6,
				nf.getUpperValue().longValue());
		assertFalse(nf.isInclusiveHigh());
		assertFalse(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"l"))).getFilter();
		assertEquals(
				4,
				nf.getLowerValue().longValue());
		assertEquals(
				6,
				nf.getUpperValue().longValue());
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

	}

	@Test
	public void testTextTypes()
			throws CQLException {
		Filter filter = CQL.toFilter("b == '10' and d like '%d' && f > '10'");
		Query query = new Query(
				"type",
				filter);

		PropertyFilterVisitor visitor = new PropertyFilterVisitor();

		PropertyConstraintSet constraints = (PropertyConstraintSet) query.getFilter().accept(
				visitor,
				null);
		TextRangeFilter tf = (TextRangeFilter) ((TextQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"b"))).getFilter();
		assertEquals(
				"10",
				tf.getEnd());
		assertEquals(
				"10",
				tf.getStart());
		assertTrue(tf.isCaseSensitive());

		LikeFilter lf = (LikeFilter) ((TextQueryConstraint) constraints.getConstraintsById(new ByteArrayId(
				"d"))).getFilter();
		assertEquals(
				"%d",
				lf.getExpression());
		assertTrue(lf.isCaseSensitive());

	}
}
