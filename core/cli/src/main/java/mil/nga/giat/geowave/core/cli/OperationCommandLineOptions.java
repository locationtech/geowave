package mil.nga.giat.geowave.core.cli;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.index.StringUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

/**
 * This class encapsulates the option for selecting the operation and parses the
 * value.
 */
public class OperationCommandLineOptions
{
	private static class CategoryKey
	{
		private final String key;
		private final CLIOperationCategory category;

		public CategoryKey(
				final CLIOperationCategory category ) {
			this.category = category;
			key = category.getCategoryId();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final CategoryKey other = (CategoryKey) obj;
			if (key == null) {
				if (other.key != null) {
					return false;
				}
			}
			else if (!key.equals(other.key)) {
				return false;
			}
			return true;
		}

	}

	private final static Logger LOGGER = Logger.getLogger(OperationCommandLineOptions.class);

	private static Map<CategoryKey, CLIOperation[]> operationRegistry = null;
	private final CLIOperation operation;

	public OperationCommandLineOptions(
			final CLIOperation operation ) {
		this.operation = operation;
	}

	public CLIOperation getOperation() {
		return operation;
	}

	public static OperationCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws IllegalArgumentException {
		CLIOperation operation = null;
		final Map<CategoryKey, CLIOperation[]> internalOperationsRegistry = getRegisteredOperations();
		for (final CLIOperation[] operations : internalOperationsRegistry.values()) {
			for (final CLIOperation o : operations) {
				if (commandLine.hasOption(o.getCommandlineOptionValue())) {
					operation = o;
					break;
				}
			}
		}
		if (operation == null) {
			final StringBuffer str = new StringBuffer();
			for (final Entry<CategoryKey, CLIOperation[]> entry : internalOperationsRegistry.entrySet()) {
				final CLIOperation[] operations = entry.getValue();
				for (int i = 0; i < operations.length; i++) {
					final CLIOperation o = operations[i];
					str.append(
							"'").append(
							o.getCommandlineOptionValue()).append(
							"'");
					if (i != (operations.length - 1)) {
						str.append(", ");
						if (i == (operations.length - 2)) {
							str.append("and ");
						}
					}
				}
			}
			LOGGER.fatal("Operation not set.  One of " + str.toString() + " must be provided");
			throw new IllegalArgumentException(
					"Operation not set.  One of " + str.toString() + " must be provided");
		}
		return new OperationCommandLineOptions(
				operation);
	}

	public static void printHelp() {
		final HelpFormatter helpFormatter = new HelpFormatter();
		final PrintWriter pw = new PrintWriter(
				new OutputStreamWriter(
						System.out,
						StringUtils.UTF8_CHAR_SET));
		final int width = HelpFormatter.DEFAULT_WIDTH;
		helpFormatter.printUsage(
				pw,
				width,
				"<operation> <options>");
		helpFormatter.printWrapped(
				pw,
				width,
				"Operations:");
		final int leftPad = HelpFormatter.DEFAULT_LEFT_PAD;
		final int descPad = HelpFormatter.DEFAULT_DESC_PAD;
		final Map<CategoryKey, CLIOperation[]> internalOperationsRegistry = getRegisteredOperations();
		for (final Entry<CategoryKey, CLIOperation[]> entry : internalOperationsRegistry.entrySet()) {
			final CLIOperation[] operations = entry.getValue();
			final Options options = new Options();
			final OptionGroup operationChoice = new OptionGroup();
			operationChoice.setRequired(true);
			for (int i = 0; i < operations.length; i++) {
				operationChoice.addOption(new Option(
						operations[i].getCommandlineOptionValue(),
						operations[i].getDescription()));
			}
			options.addOptionGroup(operationChoice);
			final CLIOperationCategory category = entry.getKey().category;

			final StringBuffer buf = new StringBuffer(
					"\n" + category.getName());
			final String description = category.getDescription();
			if ((description != null) && (description.trim().length() > 0)) {
				buf.append(
						" - ").append(
						description);
			}
			helpFormatter.printWrapped(
					pw,
					width,
					buf.toString());
			helpFormatter.printOptions(
					pw,
					width,
					options,
					leftPad,
					descPad);
		}

		helpFormatter.printWrapped(
				pw,
				width,
				"\nOptions are specific to operation choice. Use <operation> -h for help regarding options for a given operation.");
		pw.flush();
		System.exit(-1);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final OptionGroup operationChoice = new OptionGroup();
		operationChoice.setRequired(true);
		final Map<CategoryKey, CLIOperation[]> internalOperationsRegistry = getRegisteredOperations();
		for (final CLIOperation[] operations : internalOperationsRegistry.values()) {
			for (final CLIOperation o : operations) {
				operationChoice.addOption(new Option(
						o.getCommandlineOptionValue(),
						o.getDescription()));
			}
		}
		allOptions.addOptionGroup(operationChoice);
	}

	private static synchronized Map<CategoryKey, CLIOperation[]> getRegisteredOperations() {
		if (operationRegistry == null) {
			operationRegistry = new HashMap<CategoryKey, CLIOperation[]>();
			final Iterator<CLIOperationProviderSpi> operationProviders = ServiceLoader.load(
					CLIOperationProviderSpi.class).iterator();
			while (operationProviders.hasNext()) {
				final CLIOperationProviderSpi operationProvider = operationProviders.next();
				if (operationProvider != null) {
					final CLIOperationCategory category = operationProvider.getCategory();
					final CLIOperation[] newOperations = operationProvider.getOperations();
					if ((category != null) && (newOperations != null) && (newOperations.length > 0)) {
						final CategoryKey key = new CategoryKey(
								category);
						final CLIOperation[] existingOperations = operationRegistry.get(key);
						final CLIOperation[] operations;
						if ((existingOperations != null) && (existingOperations.length > 0)) {
							operations = (CLIOperation[]) ArrayUtils.addAll(
									existingOperations,
									newOperations);
						}
						else {
							operations = newOperations;
						}
						operationRegistry.put(
								key,
								operations);
					}
				}
			}
		}
		return operationRegistry;
	}
}
