package mil.nga.giat.geowave.datastore.accumulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;

public interface Transformer
{
	public Pair<Key, Value> transform(
			Pair<Key, Value> entry );
}
