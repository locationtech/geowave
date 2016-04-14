package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.util.Enumeration;
import java.util.Properties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "list", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "List property name within cache")
public class ListCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(names = {
		"-f",
		"--filter"
	})
	private String filter;

	@Override
	public void execute(
			OperationParams params ) {

		File f = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Reload options with filter if specified.
		Properties p = null;
		if (filter != null) {
			p = ConfigOptions.loadProperties(
					f,
					filter);
		}
		else {
			p = ConfigOptions.loadProperties(
					f,
					null);
		}

		JCommander.getConsole().println(
				"PROPERTIES (" + f.getName() + ")");
		Enumeration<Object> keys = p.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = (String) p.get(key);
			JCommander.getConsole().println(
					key + ": " + value);
		}
	}

}
