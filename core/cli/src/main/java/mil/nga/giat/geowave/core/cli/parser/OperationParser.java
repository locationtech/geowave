package mil.nga.giat.geowave.core.cli.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class OperationParser
{

	private final OperationRegistry registry;
	private final Set<Object> additionalObjects = new HashSet<Object>();

	public OperationParser(
			OperationRegistry registry ) {
		this.registry = registry;
	}

	public OperationParser() {
		this(
				new OperationRegistry());
	}

	/**
	 * Parse command line arguments into the given operation. The operation will
	 * be prepared, and then can be directly executed, or modified before being
	 * executed.
	 * 
	 * @param operation
	 * @param args
	 * @return
	 */
	public CommandLineOperationParams parse(
			Operation operation,
			String[] args ) {
		OperationEntry topLevelEntry = registry.getOperation(operation.getClass());
		CommandLineOperationParams params = new CommandLineOperationParams(
				Arrays.asList(new OperationEntry[] {
					topLevelEntry
				}),
				args);
		// Override the default created instance with the passed instance.
		params.getOperationMap().put(
				topLevelEntry.getOperationName(),
				operation);
		try {
			if (!doParseParameters(params)) {
				// Do not execute any commands.
				params.getOperationMap().clear();
			}
		}
		catch (ParameterException p) {
			params.setSuccessCode(-1);
			params.setSuccessMessage(p.getMessage());
			params.setSuccessException(p);
		}
		return params;

	}

	/**
	 * Search the arguments for the list of commands/operations to execute based
	 * on the top level operation entry given.
	 * 
	 * @param topLevel
	 * @param args
	 * @return
	 */
	public CommandLineOperationParams parse(
			Class<? extends Operation> topLevel,
			String[] args ) {
		OperationEntry topLevelEntry = registry.getOperation(topLevel);
		CommandLineOperationParams params = parseOperation(
				topLevelEntry,
				args);
		try {
			if (!doParseParameters(params)) {
				// Do not execute any commands.
				params.getOperationMap().clear();
			}
		}
		catch (ParameterException p) {
			params.setSuccessCode(-1);
			params.setSuccessMessage(p.getMessage());
			params.setSuccessException(p);
		}
		return params;
	}

	/**
	 * Parse the command line arguments into the objects given in the
	 * 'additionalObjects' array. I don't really ever forsee this ever being
	 * used, but hey, why not.
	 * 
	 * @param args
	 */
	public ParseOnlyOperationParams parse(
			String[] args ) {
		ParseOnlyOperationParams params = new ParseOnlyOperationParams(
				args);
		params.setAllowUnknown(true);
		doParseParameters(params);
		return params;
	}

	/**
	 * Convenience method which attempts to parse the parameters, and outputs
	 * the exception if failure.
	 * 
	 * @param parser
	 * @param params
	 * @return
	 */
	private boolean doParseParameters(
			ParseOnlyOperationParams params )
			throws ParameterException {

		// Don't validate, and allow unknown params for the prepare stage.
		params.setAllowUnknown(true);
		params.setValidate(false);

		try {
			parseParameters(params);
		}
		catch (ParameterException p) {
			throw new ParameterException(
					String.format(
							"Exception while processing arguments: %s",
							p.getMessage()),
					p);
		}

		// Re-enable param validation and don't allow unknown params
		// for the execution stage. This allows users to disable this
		// in prepare stage for whatever reason.
		params.setAllowUnknown(false);
		params.setValidate(true);

		// Prepare stage:
		for (Operation operation : params.getOperationMap().values()) {
			// Do not continue
			try {
				if (!operation.prepare(params)) {
					return false;
				}
			}
			catch (Exception p) {
				throw new ParameterException(
						String.format(
								"Unable to prepare operation: %s",
								p.getMessage()),
						p);
			}
		}

		// Final parameter parsing, for real with validation now.
		try {
			parseParameters(params);
		}
		catch (ParameterException p) {
			throw new ParameterException(
					String.format(
							"Exception while processing arguments: %s",
							p.getMessage()),
					p);
		}

		return true;
	}

	/**
	 * This function will parse the given parameters into the given operation
	 * objects. It must be a complete list. If there are any un-identified
	 * parameters, or if there are extra items specified, then it will throw an
	 * exception.
	 * 
	 * @param operations
	 * @param args
	 */
	public void parseParameters(
			ParseOnlyOperationParams params ) {

		Collection<Operation> opList = params.getOperationMap().values();

		Pair<JCommander, JCommanderTranslationMap> commander = createCommander(opList);
		JCommanderTranslationMap map = commander.getRight();
		JCommander jc = commander.getLeft();

		// Parse!
		jc.setAcceptUnknownOptions(params.isAllowUnknown());
		if (params.isValidate()) {
			jc.parse(params.getArgs());
		}
		else {
			jc.parseWithoutValidation(params.getArgs());
		}

		// This function will remove 'operation' string entries from the main
		// parameter.
		List<String> mainParameters = getAndCleanMainParameter(
				jc,
				opList.size() - 1);

		if (!params.isAllowUnknown() && !map.hasMainParameter() && mainParameters.size() > 0) {
			throw new ParameterException(
					"Unrecognized additional parameters specified: " + StringUtils.join(
							mainParameters,
							" "));
		}

		// Copy over the parsed items from the facade objects back to the real
		// objects
		map.transformToOriginal();

		params.setCommander(jc);
		params.setTranslationMap(map);
	}

	/**
	 * The point of this method is to find the list of operations that are being
	 * used on the command line.
	 * 
	 * @param args
	 */
	public CommandLineOperationParams parseOperation(
			OperationEntry topLevel,
			String[] args ) {

		// Start from the baseEntry, parsing for main parameters, until
		// we get to a point where we can't find the given operation.
		List<OperationEntry> operationEntryList = new ArrayList<OperationEntry>();
		List<Operation> opList = new ArrayList<Operation>();
		OperationEntry currentEntry = topLevel;
		while (currentEntry != null) {

			operationEntryList.add(currentEntry);
			opList.add(currentEntry.createInstance());

			Pair<JCommander, JCommanderTranslationMap> commander = createCommander(opList);
			JCommander jc = commander.getLeft();

			// Parse!
			jc.setAcceptUnknownOptions(true);
			jc.parseWithoutValidation(args);

			// Tidy up the main parameter, removing operation entries.
			List<String> mainParameters = getAndCleanMainParameter(
					jc,
					opList.size() - 1);

			// Do we have another operation?
			if (mainParameters.size() == 0) {
				break;
			}

			// See if the first item is a command...
			OperationEntry nextEntry = currentEntry.getChild(mainParameters.get(
					0).toLowerCase());
			if (nextEntry == null) {
				break;
			}

			currentEntry = nextEntry;
		}

		return new CommandLineOperationParams(
				operationEntryList,
				args);
	}

	/**
	 * This function is simply to help reduce code duplication. It creates a
	 * translator and a JCommander instance, and copies over all the parameters
	 * from the given operations to the facade objects, returning the commander
	 * + map combo as a tuple.
	 * 
	 * @param operationList
	 * @return
	 */
	private Pair<JCommander, JCommanderTranslationMap> createCommander(
			Collection<Operation> operationList ) {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		for (Operation op : operationList) {
			translator.addObject(op);
		}
		// Allow the user to extend the capabilities.
		for (Object obj : additionalObjects) {
			translator.addObject(obj);
		}
		JCommanderTranslationMap map = translator.translate();
		map.createFacadeObjects();

		// Copy default parameters over for parsing.
		map.transformToFacade();

		JCommander jc = new JCommander();
		for (Object obj : map.getObjects()) {
			jc.addObject(obj);
		}

		// If we don't have a main parameter, then that means there could be
		// an additional operation specified. Add a pseudo one to capture them.
		if (!map.hasMainParameter()) {
			MainParameterHolder holder = new MainParameterHolder();
			jc.addObject(holder);
		}
		return new MutablePair<JCommander, JCommanderTranslationMap>(
				jc,
				map);
	}

	/**
	 * This function will take the main parameter from the JCommander, and
	 * remove any 'prepended' categories and commands, which were used to parse
	 * the operations.
	 * 
	 * @param jc
	 * @param depth
	 * @return
	 */
	private List<String> getAndCleanMainParameter(
			JCommander jc,
			int depth ) {
		// Get the main parameter. We know there's at least one because we added
		// it above.
		@SuppressWarnings("unchecked")
		List<String> mainParameters = (List<String>) jc.getMainParameter().getParameterized().get(
				jc.getMainParameter().getObject());

		// Remove already executed operations
		for (int i = depth; i > 0; i--) {
			mainParameters.remove(0);
		}
		return mainParameters;
	}

	public Set<Object> getAdditionalObjects() {
		return additionalObjects;
	}

	public void addAdditionalObject(
			Object obj ) {
		additionalObjects.add(obj);
	}

	public OperationRegistry getRegistry() {
		return registry;
	}

	/**
	 * This helper class is used when there is no main parameter specified. This
	 * occurs so that we can find the next operation to execute. If there is a
	 * main parameter specified in one of the operations, then we don't add
	 * this, as it will throw a JCommander exception. That also means we can't
	 * find any further sections or commands.
	 */
	@Parameters(hidden = true)
	public static class MainParameterHolder
	{
		@Parameter
		private List<String> mainParameter = new ArrayList<String>();

		public List<String> getMainParameter() {
			return mainParameter;
		}

	}

}
