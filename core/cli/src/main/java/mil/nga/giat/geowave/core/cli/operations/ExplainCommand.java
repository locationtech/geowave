package mil.nga.giat.geowave.core.cli.operations;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;

@GeowaveOperation(name = "explain", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "See what arguments are missing and " + "what values will be used for GeoWave commands")
public class ExplainCommand implements
		Command
{

	@Override
	public boolean prepare(
			OperationParams inputParams ) {
		CommandLineOperationParams params = (CommandLineOperationParams) inputParams;
		params.setValidate(false);
		params.setAllowUnknown(true);
		// Prepared successfully.
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(
			OperationParams inputParams ) {

		CommandLineOperationParams params = (CommandLineOperationParams) inputParams;

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
		List<ParameterDescription> parameters = params.getCommander().getParameters();
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
				// Ignore, don't care
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

		// Output the main parameter.
		if (params.getCommander().getMainParameter() != null) {
			ParameterDescription pd = params.getCommander().getMainParameter();
			boolean assigned = pd.isAssigned();
			builder.append("\n");
			builder.append("Main Parameter: ");
			List<String> mP = (List<String>) pd.getParameterized().get(
					pd.getObject());
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

		JCommander.getConsole().println(
				builder.toString());
	}
}
