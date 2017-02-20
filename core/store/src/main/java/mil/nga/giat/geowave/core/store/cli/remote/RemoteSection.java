package mil.nga.giat.geowave.core.store.cli.remote;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "remote", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Operations to manage a remote store")
public class RemoteSection extends
		DefaultOperation
{
}
