package mil.nga.giat.geowave.datastore.accumulo;

import org.apache.accumulo.core.data.Key;

import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;

public class AccumuloRowId extends
		GeowaveRowId
{
	public AccumuloRowId(
			final Key key ) {
		super(
				key.getRow().copyBytes());
	}
}
