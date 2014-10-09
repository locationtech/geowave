package mil.nga.giat.geowave.accumulo;

import java.io.IOException;

import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;

public class MergingVisibilityCombiner extends
		TransformingIterator
{
	private static final byte[] AMPRISAND = StringUtils.stringToBinary("&");

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW_COLFAM_COLQUAL;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {
		Mergeable currentMergeable = null;
		Key outputKey = null;
		while (input.hasTop()) {
			final Value val = input.getTopValue();
			// the SortedKeyValueIterator uses the same instance of topKey to hold keys (a wrapper)
			final Key currentKey = new Key(input.getTopKey());
			if (outputKey == null) {
				outputKey = currentKey;
			} else if (currentMergeable != null &&
					   !outputKey.getRowData().equals(currentKey.getRowData())) {
				output.append(
						outputKey,
						new Value(
								PersistenceUtils.toBinary(currentMergeable)));
				currentMergeable = null;
				outputKey = currentKey;
				continue;
			}
			else {
				final Text combinedVisibility = new Text(
						combineVisibilities(
								currentKey.getColumnVisibility().getBytes(),
								outputKey.getColumnVisibility().getBytes()));
				outputKey = replaceColumnVisibility(
						outputKey,
						combinedVisibility);
			}
			Mergeable mergeable = PersistenceUtils.fromBinary(
					val.get(),
					Mergeable.class);
			// hopefully its never the case that null mergeables are stored,
			// but just in case, check
			mergeable = transform(
					currentKey,
					mergeable);
			if (mergeable != null) {
				if (currentMergeable == null) {
					currentMergeable = mergeable;
				}
				else {
					currentMergeable.merge(mergeable);
				}
			}
			input.next();
		}
		if (currentMergeable != null) {
			output.append(
					outputKey,
					new Value(
							PersistenceUtils.toBinary(currentMergeable)));
		}
	}

	protected Mergeable transform(
			Key key,
			final Mergeable mergeable ) {
		return mergeable;
	}

	private static byte[] combineVisibilities(
			final byte[] vis1,
			final byte[] vis2 ) {
		if (vis1 == null || vis1.length == 0) {
			return vis2;
		}
		if (vis2 == null || vis2.length == 0) {
			return vis1;
		}
		return new ColumnVisibility(
				ArrayUtils.addAll(
						ArrayUtils.addAll(
								ColumnVisibility.quote(vis1),
								AMPRISAND),
						ColumnVisibility.quote(vis2))).flatten();
	}

}
