package mil.nga.giat.geowave.core.cli.config.index;

import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.AbstractCommandObject;
import mil.nga.giat.geowave.core.cli.CLIOperation;

@Parameters(commandDescription = "Add a new index configuration to local config")
public class AddIndexOperation extends
		AbstractCommandObject implements
		CLIOperation
{
	@Parameter(description = "The name of this index")
	private List<String> indexName;
	@ParametersDelegate
	private BaseIndexConfig indexConfig;

	@Override
	public String getName() {
		return "addindex";
	}

	public BaseIndexConfig getIndexConfig() {
		return indexConfig;
	}

	@Override
	public boolean doOperation(
			JCommander commander ) {
		// TODO add index to local config
		return false;
	}
}
