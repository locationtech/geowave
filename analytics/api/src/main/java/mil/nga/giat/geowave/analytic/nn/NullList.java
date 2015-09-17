package mil.nga.giat.geowave.analytic.nn;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import com.google.common.collect.Iterators;

public class NullList<NNTYPE> implements
		NeighborList<NNTYPE>
{

	@Override
	public boolean add(
			DistanceProfile<?> distanceProfile,
			ByteArrayId id,
			NNTYPE value ) {
		return false;
	}

	@Override
	public InferType infer(
			ByteArrayId id,
			NNTYPE value ) {
		return InferType.SKIP;
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

}
