package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

public class MergingCombiner extends
		Combiner
{
	// this is "columns" because it is mimicing the behavior of
	// org.apache.accumulo.core.iterators.Combiner.setColumns()
	public static final String COLUMNS_OPTION = "columns";

	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		Mergeable currentMergeable = null;
		while (iter.hasNext()) {
			final Value val = iter.next();
			// hopefully its never the case that null stastics are stored,
			// but just in case, check
			final Mergeable mergeable = getMergeable(
					key,
					val.get());
			if (mergeable != null) {
				if (currentMergeable == null) {
					currentMergeable = mergeable;
				}
				else {
					currentMergeable.merge(mergeable);
				}
			}
		}
		if (currentMergeable != null) {
			return new Value(
					getBinary(currentMergeable));
		}
		return super.getTopValue();
	}

	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		return PersistenceUtils.fromBinary(
				binary,
				Mergeable.class);
	}

	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return PersistenceUtils.toBinary(mergeable);
	}
}
