package mil.nga.giat.geowave.mapreduce;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

public class MapReduceUtils
{
	public static List<ByteArrayId> idsFromAdapters(
			List<DataAdapter<Object>> adapters ) {
		return Lists.transform(
				adapters,
				new Function<DataAdapter<Object>, ByteArrayId>() {
					@Override
					public ByteArrayId apply(
							final DataAdapter<Object> adapter ) {
						return adapter == null ? new ByteArrayId() : adapter.getAdapterId();
					}
				});
	}
}
