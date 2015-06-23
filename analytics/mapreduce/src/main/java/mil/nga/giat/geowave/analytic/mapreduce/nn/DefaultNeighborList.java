package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class DefaultNeighborList<NNTYPE> implements
		NeighborList<NNTYPE>
{
	private final List<Entry<ByteArrayId, NNTYPE>> list = new ArrayList<Entry<ByteArrayId, NNTYPE>>();

	@Override
	public boolean add(
			final DistanceProfile<?> distanceProfile,
			final Entry<ByteArrayId, NNTYPE> entry ) {
		if (!contains(entry.getKey())) {
			list.add(entry);
			return true;
		}
		return false;

	}

	@Override
	public boolean contains(
			final ByteArrayId key ) {
		for (final Entry<ByteArrayId, NNTYPE> entry : list) {
			if (entry.getKey().equals(
					key)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public Iterator<Entry<ByteArrayId, NNTYPE>> iterator() {
		return list.iterator();
	}

	@Override
	public int size() {
		return list.size();
	}

	public static class DefaultNeighborListFactory<NNTYPE> implements
			NeighborListFactory<NNTYPE>
	{
		@Override
		public NeighborList<NNTYPE> buildNeighborList(
				final ByteArrayId centerId,
				final NNTYPE center ) {
			return new DefaultNeighborList<NNTYPE>();
		}
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	public NNTYPE get(
			final ByteArrayId key ) {
		for (final Entry<ByteArrayId, NNTYPE> entry : list) {
			if (entry.getKey().equals(
					key)) {
				return entry.getValue();
			}
		}
		return null;
	}

	@Override
	public void init() {

	}

}
