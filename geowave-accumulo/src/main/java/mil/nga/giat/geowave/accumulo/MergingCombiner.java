package mil.nga.giat.geowave.accumulo;

import java.util.Iterator;

import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.PersistenceUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

public class MergingCombiner extends
		Combiner
{
	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		Mergeable currentMergeable = null;
		while (iter.hasNext()) {
			final Value val = iter.next();
			Mergeable mergeable = PersistenceUtils.fromBinary(
					val.get(),
					Mergeable.class);
			// hopefully its never the case that null stastics are stored,
			// but just in case, check
			mergeable = transform(
					key,
					mergeable);
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
					PersistenceUtils.toBinary(currentMergeable));
		}
		return super.getTopValue();
	}

	protected Mergeable transform(
			final Key key,
			final Mergeable mergeable ) {
		return mergeable;
	}
}
