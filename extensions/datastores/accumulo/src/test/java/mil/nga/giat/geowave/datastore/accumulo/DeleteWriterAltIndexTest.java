package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Before;

public class DeleteWriterAltIndexTest extends
		DeleteWriterTest
{

	@Before
	public void setUp()
			throws IOException,
			InterruptedException,
			AccumuloException,
			AccumuloSecurityException {

		options.setUseAltIndex(true);
		super.setUp();

	}
}
