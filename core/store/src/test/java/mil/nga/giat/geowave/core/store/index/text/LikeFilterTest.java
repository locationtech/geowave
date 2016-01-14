package mil.nga.giat.geowave.core.store.index.text;

import java.util.regex.Pattern;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

import org.junit.Test;

public class LikeFilterTest
{
	@Test
	public void testSerialization() {
		final String expression = ".ox";
		final LikeFilter filter = new LikeFilter(
				expression,
				new ByteArrayId(
						StringUtils.stringToBinary("myAttribute")),
				Pattern.compile(expression.replace(
						"%",
						".*")),
				false);
		final byte[] filterBytes = PersistenceUtils.toBinary(filter);
		final LikeFilter deserializedFilter = PersistenceUtils.fromBinary(
				filterBytes,
				LikeFilter.class);
		Assert.assertTrue(filter.expression.equals(deserializedFilter.expression));
		Assert.assertTrue(filter.fieldId.equals(deserializedFilter.fieldId));
		Assert.assertTrue(filter.regex.pattern().equals(
				deserializedFilter.regex.pattern()));
		Assert.assertTrue(filter.caseSensitive == deserializedFilter.caseSensitive);
	}

	@Test
	public void testAccept() {
		final FilterableLikeConstraint constraint = new FilterableLikeConstraint(
				new ByteArrayId(
						"myAttribute"),
				"I lost my \\w+",
				true);
		final DistributableQueryFilter filter = constraint.getFilter();

		// should match expression
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										StringUtils.stringToBinary("I lost my wallet")))),
				null);

		Assert.assertTrue(filter.accept(
				null,
				persistenceEncoding));

		// should not match expression
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding2 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										StringUtils.stringToBinary("I lost his wallet")))),
				null);

		Assert.assertFalse(filter.accept(
				null,
				persistenceEncoding2));

		// should not match because of fieldId
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding3 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"mismatch"),
								new ByteArrayId(
										StringUtils.stringToBinary("I lost my wallet")))),
				null);

		Assert.assertFalse(filter.accept(
				null,
				persistenceEncoding3));
	}
}
