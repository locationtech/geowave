package mil.nga.giat.geowave.core.cli.operations.config;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class ListCommandTest {

	@Test
	public void testExecute() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(output));
		
		String[] args = {"config", "list"};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);
		ListCommand lstcommand = new ListCommand();
		lstcommand.execute(params);
		
		// can directly write to expectedoutput if knowing properties
		File f = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties p = ConfigOptions.loadProperties(
				f,
				null);
		List<String> keys = new ArrayList<String>();
		keys.addAll(p.stringPropertyNames());
		Collections.sort(keys);
		StringBuffer expectedoutput = 
				new StringBuffer("PROPERTIES (unknownversion-config.properties)\n");
		for (String key : keys) {
			String value = (String) p.get(key);
			expectedoutput.append(key + ": " + value+"\n");
		}

		assertEquals(expectedoutput.toString(), output.toString());
	}

}
