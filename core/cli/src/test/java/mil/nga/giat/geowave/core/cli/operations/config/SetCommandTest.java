package mil.nga.giat.geowave.core.cli.operations.config;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class SetCommandTest
{

	@Test
	public void testExecute() {
		String[] args = {
			"config",
			"set",
			"name",
			"value"
		};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(
				registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);

		SetCommand setcommand = new SetCommand();
		String name = "name";
		String value = "value";
		setcommand.setParameters(
				name,
				value);
		setcommand.prepare(params);
		setcommand.execute(params);

		File f = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties p = ConfigOptions.loadProperties(
				f,
				null);
		assertEquals(
				value,
				p.getProperty(name));
	}

}
