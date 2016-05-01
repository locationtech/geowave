package mil.nga.giat.geowave.core.store;

import java.util.Iterator;

public class CastIterator<T> implements
		Iterator<CloseableIterator<T>>
{

	final Iterator<CloseableIterator<Object>> it;

	public CastIterator(
			final Iterator<CloseableIterator<Object>> it ) {
		this.it = it;
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public CloseableIterator<T> next() {
		return (CloseableIterator<T>) it.next();
	}

	@Override
	public void remove() {
		it.remove();
	}
}