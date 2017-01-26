package mil.nga.giat.geowave.core.store.index.text;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

/**
 * A class based on FilterableConstraints that uses a text value for query
 * 
 * @author geowave
 *
 */

public class TextQueryConstraint implements
		FilterableConstraints
{
	private ByteArrayId fieldId;
	private String matchValue;
	private boolean caseSensitive;

	public TextQueryConstraint(
			final ByteArrayId fieldId,
			final String matchValue,
			final boolean caseSensitive ) {
		super();
		this.fieldId = fieldId;
		this.matchValue = matchValue;
		this.caseSensitive = caseSensitive;
	}

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new TextExactMatchFilter(
				fieldId,
				matchValue,
				caseSensitive);
	}

	public List<ByteArrayRange> getRange() {
		// TODO case sensitivity
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						matchValue),
				new ByteArrayId(
						matchValue)));
	}

	@Override
	public FilterableConstraints intersect(
			FilterableConstraints constaints ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FilterableConstraints union(
			FilterableConstraints constaints ) {
		// TODO Auto-generated method stub
		return null;
	}

}
