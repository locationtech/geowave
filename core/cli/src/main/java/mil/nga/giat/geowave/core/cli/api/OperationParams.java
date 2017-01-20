package mil.nga.giat.geowave.core.cli.api;

import java.util.Map;

/**
 * This arguments are used to allow sections and commands to modify how
 * arguments are parsed during prepare / execution stage.
 */
public interface OperationParams
{

	/**
	 * Operations that were parsed & instantiated for execution
	 * 
	 * @return
	 */
	Map<String, Operation> getOperationMap();

	/**
	 * Key value pairs for contextual information during command parsing
	 */
	Map<String, Object> getContext();
}
