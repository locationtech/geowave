package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;

import org.junit.Before;

public class DeleteWriterAltIndexTest extends
		DeleteWriterTest
{

	@Before
	public void setUp()
			throws IOException {

		options.setUseAltIndex(true);
		super.setUp();

	}
}
