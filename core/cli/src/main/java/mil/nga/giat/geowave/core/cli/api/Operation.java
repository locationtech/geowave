package mil.nga.giat.geowave.core.cli.api;

/**
 * An operation in GeoWave is something that can be prepared() and executed().
 * The prepare() function will look at parameters and based on their values, set
 * 
 * @ParametersDelegate classes which can soak up more parameters. Then, the
 *                     parameters are parsed again before being fed into the
 *                     execute() command, if the operation also implements
 *                     Command.
 */
public interface Operation
{
	/**
	 * NOTE: ONLY USE THIS METHOD TO SET @PARAMETERSDELEGATE options. If you
	 * throw exceptions or do validation, then it will make help/explain
	 * commands not work correctly.
	 */
	boolean prepare(
			OperationParams params );
}
