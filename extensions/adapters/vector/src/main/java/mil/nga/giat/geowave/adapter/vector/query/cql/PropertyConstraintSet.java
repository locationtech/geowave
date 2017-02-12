package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;

public class PropertyConstraintSet
{
	private final Map<ByteArrayId, FilterableConstraints> constraints = new HashMap<ByteArrayId, FilterableConstraints>();

	public PropertyConstraintSet() {}

	public PropertyConstraintSet(
			final FilterableConstraints constraint ) {
		add(
				constraint,
				true);
	}

	public boolean isEmpty() {
		return constraints.isEmpty();
	}

	public List<FilterableConstraints> getConstraintsFor(
			final ByteArrayId[] fieldIds ) {
		final List<FilterableConstraints> result = new LinkedList<FilterableConstraints>();
		for (final ByteArrayId fieldId : fieldIds) {
			final FilterableConstraints c = constraints.get(fieldId);
			if (c != null) {
				result.add(c);
			}

		}
		return result;
	}

	public List<ByteArrayRange> getRangesFor(
			final SecondaryIndex<?> index ) {
		final List<ByteArrayRange> result = new LinkedList<ByteArrayRange>();
		final FilterableConstraints c = constraints.get(index.getFieldId());
		if (c != null) {
			result.addAll(index.getIndexStrategy().getQueryRanges(
					c));
		}
		return result;
	}

	public List<DistributableQueryFilter> getFiltersFor(
			final SecondaryIndex<?> index ) {
		final List<DistributableQueryFilter> result = new LinkedList<DistributableQueryFilter>();
		final FilterableConstraints c = constraints.get(index.getFieldId());
		if (c != null) {
			final DistributableQueryFilter filter = c.getFilter();
			if (filter != null) {
				result.add(filter);
			}
		}
		return result;
	}

	public void add(
			final FilterableConstraints constraint,
			final boolean intersect ) {
		final ByteArrayId id = constraint.getFieldId();
		final FilterableConstraints constraintsForId = constraints.get(id);
		if (constraintsForId == null) {
			constraints.put(
					id,
					constraint);
		}
		else if (intersect) {
			constraints.put(
					id,
					constraintsForId.intersect(constraint));
		}
		else {
			constraints.put(
					id,
					constraintsForId.union(constraint));
		}
	}

	public void intersect(
			final PropertyConstraintSet set ) {
		for (final Map.Entry<ByteArrayId, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					true);
		}
	}

	public void union(
			final PropertyConstraintSet set ) {
		for (final Map.Entry<ByteArrayId, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					false);
		}
	}

	public FilterableConstraints getConstraintsById(
			final ByteArrayId id ) {
		return constraints.get(id);
	}

}
