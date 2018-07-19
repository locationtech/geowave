package mil.nga.giat.geowave.mapreduce;

import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class MapReduceMemoryOperations extends MemoryDataStoreOperations implements MapReduceDataStoreOperations
{

	@Override
	public <T> Reader<T> createReader(
			RecordReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

}
