package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.shaded.restlet.resource.Get;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

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

		Pair<String, Properties> list = getList(params);
		String name = list.getKey();
		Properties p = list.getValue();

		JCommander.getConsole().println(
				"PROPERTIES (" + name + ")");

		List<String> keys = new ArrayList<String>();
		keys.addAll(p.stringPropertyNames());
		Collections.sort(keys);

		for (String key : keys) {
			String value = (String) p.get(key);
			JCommander.getConsole().println(
					key + ": " + value);
		}
	}

	@Get("json")
	public Properties restGet() {
		filter = getQueryValue("filter");

		OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				ConfigOptions.getDefaultPropertyFile());
		return getList(
				params).getValue();
	}

	private Pair<String, Properties> getList(
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

		return new ImmutablePair<>(
				f.getName(),
				p);
	}

}
