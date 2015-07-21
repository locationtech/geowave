package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import com.google.common.collect.Iterators;

public class NullList<NNTYPE> implements
		NeighborList<NNTYPE>
{

	@Override
	public boolean add(
			final DistanceProfile<?> distanceProfile,
			final Entry<ByteArrayId, NNTYPE> entry ) {
		return false;
	}

	@Override
	public boolean contains(
			final ByteArrayId key ) {
		return false;
	}

	@Override
	public void clear() {

	}

	@Override
	public Iterator<Entry<ByteArrayId, NNTYPE>> iterator() {
		return Iterators.emptyIterator();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public void init() {

	}

}
