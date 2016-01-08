package mil.nga.giat.geowave.core.store.index.text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CompositeConstraints;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public class FilterableLikeConstraint extends
		TextQueryConstraint
{

	private final String expression;
	private final ByteArrayId fieldId;
	private final Pattern regex;
	private final boolean caseSensitive;

	/*
	 * Equals
	 */
	public FilterableLikeConstraint(
			final ByteArrayId fieldId,
			final String expression,
			final boolean caseSensitive ) {
		this.expression = expression;
		this.fieldId = fieldId;
		this.caseSensitive = caseSensitive;
		regex = Pattern.compile(
				expression.replaceAll(
						"%",
						".*"),
				caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public List<ByteArrayRange> getRange(
			final int minNGramSize,
			final int maxNGramSize ) {
		final int percentIndex = expression.indexOf('%');
		if (percentIndex == 0) {
			// ends with case
			int count = 0;
			int maxSize = 0;
			for (int i = 0; i < expression.length(); i++) {
				if (expression.charAt(i) == '%') {
					count = 0;
				}
				count++;
				maxSize = Math.max(
						maxSize,
						count);
			}
			// find the largest possible ngrams
			final int minNGramSearchSize = Math.min(
					maxNGramSize,
					Math.max(
							minNGramSize,
							maxSize));
			final List<ByteArrayId> specificNGrams = TextIndexStrategy.grams(
					expression.toLowerCase(),
					minNGramSearchSize,
					maxNGramSize);
			final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
			for (final ByteArrayId id : specificNGrams) {
				ranges.add(new ByteArrayRange(
						id,
						id));
				break; // actually only need to pick one...but the best one
						// based on stats
			}
			return ranges;
		}
		else if (percentIndex > 0) {
			// starts with case
			final String prefix = expression.substring(
					0,
					percentIndex).toLowerCase();
			final String startSearchString = (prefix.length() == (expression.length() - 1)) ? prefix : prefix + "\000";
			final String endSearchString = (prefix.length() == (expression.length() - 1)) ? prefix : prefix + "\177";
			return new FilterableTextRangeConstraint(
					fieldId,
					startSearchString,
					endSearchString,
					false).getRange(
					minNGramSize,
					maxNGramSize);
		}
		else {
			return new FilterableTextRangeConstraint(
					fieldId,
					expression.toLowerCase(),
					false).getRange(
					minNGramSize,
					maxNGramSize);
		}
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new LikeFilter(
				expression,
				fieldId,
				regex,
				caseSensitive);
	}

	@Override
	public FilterableConstraints intersect(
			FilterableConstraints constraints ) {
		final FilterableLikeConstraint flc = (FilterableLikeConstraint) constraints;
		final CompositeConstraints cc = new CompositeConstraints(
				Arrays.asList(
						(FilterableConstraints) this,
						(FilterableConstraints) flc),
				true);
		return cc;
	}

	@Override
	public FilterableConstraints union(
			FilterableConstraints constraints ) {
		final FilterableLikeConstraint flc = (FilterableLikeConstraint) constraints;
		final CompositeConstraints cc = new CompositeConstraints(
				Arrays.asList(
						(FilterableConstraints) this,
						(FilterableConstraints) flc));
		return cc;
	}
}