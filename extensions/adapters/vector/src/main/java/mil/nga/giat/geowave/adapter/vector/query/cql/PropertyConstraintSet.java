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
	private Map<ByteArrayId, FilterableConstraints> constraints = new HashMap<ByteArrayId, FilterableConstraints>();

	public PropertyConstraintSet() {}

	public PropertyConstraintSet(
			FilterableConstraints constraint ) {
		add(
				constraint,
				true);
	}

	public List<FilterableConstraints> getConstraintsFor(
			ByteArrayId[] fieldIds ) {
		List<FilterableConstraints> result = new LinkedList<FilterableConstraints>();
		for (ByteArrayId fieldId : fieldIds) {
			final FilterableConstraints c = constraints.get(fieldId);
			if (c != null) result.add(c);

		}
		return result;
	}

	public List<ByteArrayRange> getRangesFor(
			SecondaryIndex<?> index ) {
		List<ByteArrayRange> result = new LinkedList<ByteArrayRange>();
		for (ByteArrayId fieldId : index.getFieldIDs()) {
			final FilterableConstraints c = constraints.get(fieldId);
			if (c != null) result.addAll(index.getIndexStrategy().getQueryRanges(
					c));
		}
		return result;
	}

	public List<DistributableQueryFilter> getFiltersFor(
			SecondaryIndex<?> index ) {
		List<DistributableQueryFilter> result = new LinkedList<DistributableQueryFilter>();
		for (ByteArrayId fieldId : index.getFieldIDs()) {
			final FilterableConstraints c = constraints.get(fieldId);
			if (c != null) result.add(c.getFilter());
		}
		return result;
	}

	public void add(
			FilterableConstraints constraint,
			boolean intersect ) {
		final ByteArrayId id = constraint.getFieldId();
		FilterableConstraints constraintsForId = constraints.get(id);
		if (constraintsForId == null) {
			constraints.put(
					id,
					constraint);
		}
		else if (intersect)
			constraints.put(
					id,
					constraintsForId.intersect(constraint));

		else
			constraints.put(
					id,
					constraintsForId.union(constraint));
	}

	public void intersect(
			PropertyConstraintSet set ) {
		for (Map.Entry<ByteArrayId, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					true);
		}
	}

	public void union(
			PropertyConstraintSet set ) {
		for (Map.Entry<ByteArrayId, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					false);
		}
	}

	public FilterableConstraints getConstraintsById(
			ByteArrayId id ) {
		return constraints.get(id);
	}

}
