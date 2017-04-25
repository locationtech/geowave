package mil.nga.giat.geowave.core.store.operations.config;

import org.shaded.restlet.representation.Representation;

import java.io.File;

import org.shaded.restlet.data.Form;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.resource.Post;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexGroupPluginOptions;
import static mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation.RestEnabledType.*;

@GeowaveOperation(name = "rmindexgrp", parentOperation = ConfigSection.class, restEnabled = POST)
@Parameters(commandDescription = "Remove index group from Geowave configuration")
public class RemoveIndexGroupCommand extends
		AbstractRemoveCommand implements
		Command
{

	@Override
	public Void computeResults(
			OperationParams params ) {

		// Search for properties relevant to the given name
		pattern = IndexGroupPluginOptions.getIndexGroupNamespace(getEntryName());
		return super.computeResults(params);

	}

	@Override
	public void execute(
			OperationParams params ) {
		computeResults(params);
	}
}
