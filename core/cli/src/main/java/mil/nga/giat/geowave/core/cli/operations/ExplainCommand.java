package mil.nga.giat.geowave.core.cli.operations;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;

@GeowaveOperation(name = "explain", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "See what arguments are missing and "
		+ "what values will be used for GeoWave commands")
public class ExplainCommand extends
		DefaultOperation implements
		Command
{

	private static Logger LOGGER = LoggerFactory.getLogger(ExplainCommand.class);

	@Override
	public boolean prepare(
			OperationParams inputParams ) {
		super.prepare(inputParams);
		CommandLineOperationParams params = (CommandLineOperationParams) inputParams;
		params.setValidate(false);
		params.setAllowUnknown(true);
		// Prepared successfully.
		return true;
	}

	@Override
	public void execute(
			OperationParams inputParams ) {

		CommandLineOperationParams params = (CommandLineOperationParams) inputParams;

		StringBuilder builder = new StringBuilder();

		// Sort first
		String nextCommand = "geowave";
		JCommander commander = params.getCommander();
		while (commander != null) {
			if (commander.getParameters() != null && commander.getParameters().size() > 0) {
				builder.append("Command: ");
				builder.append(nextCommand);
				builder.append(" [options]");
				if (commander.getParsedCommand() != null) {
					builder.append(" <subcommand> ...");
				}
				builder.append("\n\n");
				builder.append(explainCommander(commander));
				builder.append("\n");
			}
			else if (commander.getMainParameter() != null) {
				builder.append("Command: ");
				builder.append(nextCommand);
				if (commander.getParsedCommand() != null) {
					builder.append(" <subcommand> ...");
				}
				builder.append("\n\n");
				builder.append(explainMainParameter(commander));
				builder.append("\n");
			}
			nextCommand = commander.getParsedCommand();
			commander = commander.getCommands().get(
					nextCommand);
		}

		JCommander.getConsole().println(
				builder.toString().trim());
	}

	/**
	 * This function will explain the currently selected values for a
	 * JCommander.
	 * 
	 * @param commander
	 */
	public static StringBuilder explainCommander(
			JCommander commander ) {

		StringBuilder builder = new StringBuilder();

		builder.append(" ");
		builder.append(String.format(
				"%1$20s",
				"VALUE"));
		builder.append("  ");
		builder.append("NEEDED  ");
		builder.append(String.format(
				"%1$-40s",
				"PARAMETER NAMES"));
		builder.append("\n");
		builder.append("----------------------------------------------\n");

		// Sort first
		SortedMap<String, ParameterDescription> parameterDescs = new TreeMap<String, ParameterDescription>();
		List<ParameterDescription> parameters = commander.getParameters();
		for (ParameterDescription pd : parameters) {
			parameterDescs.put(
					pd.getLongestName(),
					pd);
		}

		// Then output
		for (ParameterDescription pd : parameterDescs.values()) {

			Object value = null;
			try {
				// value = tEntry.getParam().get(tEntry.getObject());
				value = pd.getParameterized().get(
						pd.getObject());
			}
			catch (Exception e) {
				LOGGER.warn(
						"Unable to set value",
						e);
			}

			boolean required = false;
			if (pd.getParameterized().getParameter() != null) {
				required = pd.getParameterized().getParameter().required();
			}
			else if (pd.isDynamicParameter()) {
				required = pd.getParameter().getDynamicParameter().required();
			}

			String names = pd.getNames();
			boolean assigned = pd.isAssigned();

			// Data we have:
			// required, assigned, value, names.
			builder.append("{");
			if (value == null) {
				value = "";
			}
			builder.append(String.format(
					"%1$20s",
					value));
			builder.append("} ");
			if (required && !assigned) {
				builder.append("MISSING ");
			}
			else {
				builder.append("        ");
			}
			builder.append(String.format(
					"%1$-40s",
					StringUtils.join(
							names,
							",")));
			builder.append("\n");

		}

		if (commander.getMainParameter() != null) {
			builder.append("\n");
			builder.append(explainMainParameter(commander));
		}

		return builder;
	}

	/**
	 * Output details about the main parameter, if there is one.
	 * 
	 * @param commander
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static StringBuilder explainMainParameter(
			JCommander commander ) {
		StringBuilder builder = new StringBuilder();

		ParameterDescription mainParameter = commander.getMainParameter();

		// Output the main parameter.
		if (mainParameter != null) {
			if (mainParameter.getDescription() != null && mainParameter.getDescription().length() > 0) {
				builder.append("Expects: ");
				builder.append(mainParameter.getDescription());
				builder.append("\n");
			}

			boolean assigned = mainParameter.isAssigned();
			builder.append("Specified: ");
			List<String> mP = (List<String>) mainParameter.getParameterized().get(
					mainParameter.getObject());
			if (!assigned || mP.size() == 0) {
				builder.append("<none specified>");
			}
			else {
				builder.append(String.format(
						"%n%s",
						StringUtils.join(
								mP,
								" ")));
			}
			builder.append("\n");
		}

		return builder;
	}

}
