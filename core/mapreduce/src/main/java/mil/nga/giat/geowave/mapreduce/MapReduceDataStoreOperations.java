package mil.nga.giat.geowave.mapreduce;

import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public interface MapReduceDataStoreOperations extends
		DataStoreOperations
{
	public Reader createReader(
			RecordReaderParams readerParams );

}
