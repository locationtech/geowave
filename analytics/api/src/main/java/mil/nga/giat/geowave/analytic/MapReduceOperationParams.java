package mil.nga.giat.geowave.analytic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.core.cli.api.JCommanderOperationParams;
import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;

public class MapReduceOperationParams implements
		JCommanderOperationParams
{

	private final OperationEntry operationEntry;
	private final Map<String, Operation> operationMap = new HashMap<String, Operation>();
	private final Map<String, Object> context = new HashMap<String, Object>();
	private final String[] args;
	private JCommander commander;
	private JCommanderTranslationMap translationMap;

	public MapReduceOperationParams(
			OperationEntry entry,
			String[] args ) {
		this.operationEntry = entry;
		this.operationMap.put(
				entry.getOperationName(),
				entry.createInstance());
		this.args = args;
		doParse();
	}

	@Override
	public Map<String, Operation> getOperationMap() {
		return Collections.unmodifiableMap(operationMap);
	}

	@Override
	public Map<String, Object> getContext() {
		return context;
	}

	public OperationEntry getOperationEntry() {
		return operationEntry;
	}

	@Override
	public JCommander getCommander() {
		return commander;
	}

	@Override
	public JCommanderTranslationMap getTranslationMap() {
		return translationMap;
	}

	public String[] getArgs() {
		return this.args;
	}

	/**
	 * Create a commander and parse the given arguments. Prepare the operation,
	 * and parse the arguments again.
	 */
	private void doParse() {
		Operation operation = operationMap.get(operationEntry.getOperationName());
		// Get options to load plugins
		doParseStage(operation);
		// Prepare (set plugin options)
		operation.prepare(this);
		// Plugin options.
		doParseStage(operation);
	}

	/**
	 * Parse the options. TODO: can i combine this with the OperationParser
	 * logic?
	 * 
	 * @param operation
	 */
	private void doParseStage(
			Operation operation ) {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(operation);
		translationMap = translator.translate();
		translationMap.createFacadeObjects();
		translationMap.transformToFacade();
		commander = new JCommander();
		for (Object obj : translationMap.getObjects()) {
			commander.addObject(obj);
		}
		// MapReduce options will cause us to fail, so allow unknown.
		commander.setAcceptUnknownOptions(true);
		commander.parse(getArgs());
		translationMap.transformToOriginal();
	}
}
