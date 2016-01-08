package mil.nga.giat.geowave.core.store.index;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class CompositeConstraints implements
		FilterableConstraints
{
	private final List<FilterableConstraints> constraints = new LinkedList<FilterableConstraints>();
	private boolean intersect = false;

	public CompositeConstraints() {}

	public CompositeConstraints(
			final List<FilterableConstraints> constraints ) {
		super();
		this.constraints.addAll(constraints);
	}

	public CompositeConstraints(
			final List<FilterableConstraints> constraints,
			final boolean intersect ) {
		super();
		this.constraints.addAll(constraints);
		this.intersect = intersect;
	}

	public List<FilterableConstraints> getConstraints() {
		return constraints;
	}

	@Override
	public int getDimensionCount() {
		return constraints == null ? 0 : constraints.size();
	}

	@Override
	public boolean isEmpty() {
		return (constraints == null) || constraints.isEmpty();
	}

	@Override
	public DistributableQueryFilter getFilter() {
		final List<DistributableQueryFilter> filters = new ArrayList<DistributableQueryFilter>();
		for (final QueryConstraints constraint : constraints) {
			if (constraint instanceof FilterableConstraints) {
				filters.add(((FilterableConstraints) constraint).getFilter());
			}
		}
		return new DistributableFilterList(
				intersect,
				filters);
	}

	@Override
	public ByteArrayId getFieldId() {
		return constraints.get(
				0).getFieldId();
	}

	@Override
	public FilterableConstraints intersect(
			final FilterableConstraints constraints ) {
		final CompositeConstraints cc = new CompositeConstraints(
				this.constraints,
				true);
		cc.constraints.add(constraints);
		return cc;
	}

	@Override
	public FilterableConstraints union(
			final FilterableConstraints constraints ) {
		final CompositeConstraints cc = new CompositeConstraints(
				this.constraints);
		cc.constraints.add(constraints);
		return cc;
	}

}
