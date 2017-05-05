package mil.nga.giat.geowave.core.cli.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

@GeowaveOperation(name = "help", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Get descriptions of arguments for " + "any GeoWave command")
public class HelpCommand extends
		DefaultOperation implements
		Command
{

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

		List<String> nameArray = new ArrayList<String>();
		OperationRegistry registry = OperationRegistry.getInstance();

		StringBuilder builder = new StringBuilder();

		Operation lastOperation = null;
		for (Map.Entry<String, Operation> entry : params.getOperationMap().entrySet()) {
			if (entry.getValue() == this) {
				continue;
			}
			nameArray.add(entry.getKey());
			lastOperation = entry.getValue();
		}

		if (lastOperation == null) {
			lastOperation = registry.getOperation(
					GeowaveTopLevelSection.class).createInstance();
		}
		if (lastOperation != null) {
			String usage = lastOperation.usage();
			if (usage != null) {
				System.out.println(usage);
			}
			else {
				// This is done because if we don't, then JCommander will
				// consider the given parameters as the Default parameters.
				// It's also done so that we can parse prefix annotations
				// and special delegate processing.
				JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();

				translator.addObject(lastOperation);
				JCommanderTranslationMap map = translator.translate();
				map.createFacadeObjects();

				// Copy default parameters over for help display.
				map.transformToFacade();

				// Execute a prepare

				// Add processed objects
				JCommander jc = new JCommander();
				for (Object obj : map.getObjects()) {
					jc.addObject(obj);
				}

				String programName = StringUtils.join(
						nameArray,
						" ");
				jc.setProgramName(programName);
				jc.usage(builder);

				// Trim excess newlines.
				String operations = builder.toString().trim();
				builder = new StringBuilder();
				builder.append(operations);
				builder.append("\n\n");

				// Add sub-commands
				OperationEntry lastEntry = registry.getOperation(lastOperation.getClass());
				// Cast to list so we can sort it based on operation name.
				List<OperationEntry> children = new ArrayList<OperationEntry>(
						lastEntry.getChildren());
				Collections.sort(
						children,
						getOperationComparator());
				if (children.size() > 0) {
					builder.append("  Commands:\n");
					for (OperationEntry childEntry : children) {

						// Get description annotation
						Parameters p = childEntry.getOperationClass().getAnnotation(
								Parameters.class);

						// If not hidden, then output it.
						if (p == null || !p.hidden()) {
							builder.append(String.format(
									"    %s%n",
									childEntry.getOperationName()));
							if (p != null) {
								String description = p.commandDescription();
								builder.append(String.format(
										"      %s%n",
										description));
							}
							else {
								builder.append("      <no description>\n");
							}
							builder.append("\n");
						}
					}
				}

				// Trim excess newlines.
				String output = builder.toString().trim();

				System.out.println(output);
			}
		}
	}

	/**
	 * This will sort operations based on their name. Just looks prettier on
	 * output.
	 * 
	 * @return
	 */
	private Comparator<OperationEntry> getOperationComparator() {
		return new Comparator<OperationEntry>() {
			@Override
			public int compare(
					OperationEntry o1,
					OperationEntry o2 ) {
				return o1.getOperationName().compareTo(
						o2.getOperationName());
			}
		};
	}
}
