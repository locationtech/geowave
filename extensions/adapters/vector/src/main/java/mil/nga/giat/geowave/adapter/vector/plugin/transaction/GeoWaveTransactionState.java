package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;

import org.geotools.data.Transaction.State;

public interface GeoWaveTransactionState extends
		State
{
	public GeoWaveTransaction getGeoWaveTransaction(
			String typeName )
			throws IOException;
}
