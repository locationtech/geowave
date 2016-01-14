package mil.nga.giat.geowave.core.store.filter;

import java.util.List;

import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

/**
 * This class wraps a list of filters into a single filter such that if any one
 * filter fails this class will fail acceptance.
 * 
 * @param <T>
 */
public class FilterList<T extends QueryFilter> implements
		QueryFilter
{
	protected List<T> filters;
	protected boolean logicalAnd = true;

	protected FilterList() {}

	protected FilterList(
			boolean logicalAnd ) {
		this.logicalAnd = logicalAnd;
	}

	public FilterList(
			final List<T> filters ) {
		this.filters = filters;
	}

	public FilterList(
			boolean logicalAnd,
			final List<T> filters ) {
		this.logicalAnd = logicalAnd;
		this.filters = filters;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> entry ) {
		for (final QueryFilter filter : filters) {
			final boolean ok = filter.accept(
					indexModel,
					entry);
			if (!ok && logicalAnd) return false;
			if (ok && !logicalAnd) return true;

		}
		return logicalAnd;
	}

}
