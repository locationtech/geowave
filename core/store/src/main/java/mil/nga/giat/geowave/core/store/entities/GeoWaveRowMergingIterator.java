package mil.nga.giat.geowave.core.store.entities;

import java.util.Iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class GeoWaveRowMergingIterator<T extends MergeableGeoWaveRow> implements
		Iterator<T>
{

	final Iterator<T> source;
	final PeekingIterator<T> peekingIterator;

	public GeoWaveRowMergingIterator(
			final Iterator<T> source ) {
		this.source = source;
		this.peekingIterator = Iterators.peekingIterator(source);
	}

	@Override
	public boolean hasNext() {
		return peekingIterator.hasNext();
	}

	@Override
	public T next() {
		final T nextValue = peekingIterator.next();
		while (peekingIterator.hasNext() && nextValue.shouldMerge(peekingIterator.peek())) {
			nextValue.mergeRow(peekingIterator.next());
		}
		return nextValue;
	}
}
