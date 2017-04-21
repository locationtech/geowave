package mil.nga.giat.geowave.core.cli.api;

/**
 * An operation in GeoWave is something that can be prepared() and executed().
 * The prepare() function will look at parameters and based on their values, set
 * classes which can soak up more parameters. Then, the parameters are parsed
 * again before being fed into the execute() command, if the operation also
 * implements Command.
 */
public interface Operation
{
	/*
	 * NOTE: ONLY USE THIS METHOD TO SET @PARAMETERSDELEGATE options. If you
	 * throw exceptions or do validation, then it will make help/explain
	 * commands not work correctly.
	 */
	/**
	 * Prepare the command, and return whether we want to continue preparation
	 * 
	 * @param params
	 *            Arguments to be used to allow sections and commands to modify
	 *            how arguments are parsed during prepare stage.
	 * @return If prepare was successful.
	 */
	boolean prepare(
			OperationParams params );
}
