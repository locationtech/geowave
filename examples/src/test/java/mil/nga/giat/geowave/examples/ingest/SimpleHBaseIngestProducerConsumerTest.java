package mil.nga.giat.geowave.examples.ingest;

import org.junit.Test;

public class SimpleHBaseIngestProducerConsumerTest extends
		SimpleHBaseIngestTest
{
	@Test
	public void TestIngest() {
		final SimpleIngestProducerConsumer si = new SimpleIngestProducerConsumer();
		si.generateGrid(mockDataStore);
		validate(mockDataStore);
	}

}