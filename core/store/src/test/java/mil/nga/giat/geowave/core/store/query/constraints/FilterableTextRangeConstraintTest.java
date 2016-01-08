package mil.nga.giat.geowave.core.store.query.constraints;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.text.FilterableTextRangeConstraint;

import org.junit.Test;

public class FilterableTextRangeConstraintTest
{
	final ByteArrayId fieldID = new ByteArrayId(
			"field");

	private IndexedPersistenceEncoding<ByteArrayId> create(
			final String value ) {
		return new IndexedPersistenceEncoding<ByteArrayId>(
				fieldID,
				fieldID,
				fieldID,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								fieldID,
								new ByteArrayId(
										StringUtils.stringToBinary(value)))),
				null);
	}

	@Test
	public void testOne() {
		FilterableTextRangeConstraint constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				true);
		QueryFilter filter = constraint.getFilter();
		assertFalse(filter.accept(
				null,
				create("fReddog")));
		assertTrue(filter.accept(
				null,
				create("RedDog")));

		constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				false);
		filter = constraint.getFilter();
		assertFalse(filter.accept(
				null,
				create("fReddog")));
		assertTrue(filter.accept(
				null,
				create("RedDog")));
		assertTrue(filter.accept(
				null,
				create("reddog")));
	}

	@Test
	public void testRange() {
		FilterableTextRangeConstraint constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				"SadDog",
				true);
		QueryFilter filter = constraint.getFilter();
		assertFalse(filter.accept(
				null,
				create("fReddog")));
		assertTrue(filter.accept(
				null,
				create("RedDog")));
		assertTrue(filter.accept(
				null,
				create("RodDog")));
		assertFalse(filter.accept(
				null,
				create("SidDog")));

		constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				"SadDog",
				false);
		filter = constraint.getFilter();
		assertFalse(filter.accept(
				null,
				create("fReddog")));
		assertTrue(filter.accept(
				null,
				create("RedDog")));
		assertTrue(filter.accept(
				null,
				create("roddOg")));
		assertTrue(filter.accept(
				null,
				create("ridDog")));
	}

}
