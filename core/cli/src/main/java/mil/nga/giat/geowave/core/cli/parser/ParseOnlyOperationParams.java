package mil.nga.giat.geowave.core.cli.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.core.cli.api.JCommanderOperationParams;
import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;

public class ParseOnlyOperationParams implements
		JCommanderOperationParams
{

	private final Map<String, Object> context = new HashMap<String, Object>();
	private final String[] args;
	private JCommander commander;
	private JCommanderTranslationMap translationMap;
	private boolean validate = true;
	private boolean allowUnknown = false;

	public ParseOnlyOperationParams(
			String[] args ) {
		this.args = args;
	}

	public String[] getArgs() {
		return this.args;
	}

	@Override
	public Map<String, Object> getContext() {
		return this.context;
	}

	@Override
	public JCommander getCommander() {
		return this.commander;
	}

	@Override
	public JCommanderTranslationMap getTranslationMap() {
		return this.translationMap;
	}

	public void setValidate(
			boolean validate ) {
		this.validate = validate;
	}

	public void setAllowUnknown(
			boolean allowUnknown ) {
		this.allowUnknown = allowUnknown;
	}

	public boolean isValidate() {
		return validate;
	}

	public boolean isAllowUnknown() {
		return this.allowUnknown;
	}

	public void setTranslationMap(
			JCommanderTranslationMap translationMap ) {
		this.translationMap = translationMap;
	}

	public void setCommander(
			JCommander commander ) {
		this.commander = commander;
	}

	/**
	 * ParseOnly has no operations.
	 */
	@Override
	public Map<String, Operation> getOperationMap() {
		return Collections.unmodifiableMap(new HashMap<String, Operation>());
	}

}
