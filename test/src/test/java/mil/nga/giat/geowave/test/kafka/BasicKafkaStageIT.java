package mil.nga.giat.geowave.test.kafka;

import org.apache.log4j.Logger;
import org.junit.Test;

public class BasicKafkaStageIT extends
		KafkaTestBase
{
	private final static Logger LOGGER = Logger.getLogger(BasicKafkaStageIT.class);

	@Test
	public void testBasicStageGpx()
			throws Exception {

		testKafkaStage(OSM_GPX_INPUT_DIR);

	}
}
