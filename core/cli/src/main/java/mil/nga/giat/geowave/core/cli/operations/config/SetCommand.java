package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "set", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Set property name within cache")
public class SetCommand extends
		DefaultOperation implements
		Command
{

	@Parameter
	private List<String> opts = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params ) {

		File f = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties p = ConfigOptions.loadProperties(
				f,
				null);

		String key = null;
		String value = null;
		if (opts.size() == 1) {
			String[] parts = StringUtils.split(
					opts.get(0),
					"=");
			key = parts[0];
			value = parts[1];
		}
		else {
			key = opts.get(0);
			value = opts.get(1);
		}

		p.setProperty(
				key,
				value);
		ConfigOptions.writeProperties(
				f,
				p);
	}

}
