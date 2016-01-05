package mil.nga.giat.geowave.core.cli;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import com.beust.jcommander.JCommander;

public class OperationRegistry
{
	private final Map<String, InternalOperationCategory> categoryMap;

	private OperationRegistry(
			final Map<String, InternalOperationCategory> categoryMap ) {
		this.categoryMap = categoryMap;
	}

	public static OperationRegistry loadRegistry() {
		final Iterator<CLIOperationProviderSpi> operationProviders = ServiceLoader.load(
				CLIOperationProviderSpi.class).iterator();
		final Map<String, InternalOperationCategory> categoryMap = new HashMap<String, InternalOperationCategory>();
		while (operationProviders.hasNext()) {
			final CLIOperationProviderSpi operationProvider = operationProviders.next();
			if (operationProvider != null) {
				final CLIOperation[] operations = operationProvider.createOperations();

				if ((operations != null) && (operations.length > 0)) {
					final CommandObject category = operationProvider.getOperationCategory();
					InternalOperationCategory internalCategory = categoryMap.get(
							category.getName());
					if (internalCategory == null) {
						internalCategory = new InternalOperationCategory(
								category);
						categoryMap.put(
								category.getName(),
								internalCategory);
					}
					for (final CLIOperation op : operations) {
						if (internalCategory.opsMap.containsKey(
								op.getName())) {
							JCommander.getConsole().println(
									"Duplicate operation registered with name '" + op.getName() + "'.  Registered class is '" + internalCategory.opsMap.get(
											op.getName()).op.getClass().getName() + "'. Ignoring class '" + op.getClass().getName() + "'");
							continue;
						}

						final InternalOperation internalOp = new InternalOperation(
								op);
						internalCategory.opsMap.put(
								op.getName(),
								internalOp);
					}
				}
			}
		}
		return new OperationRegistry(
				categoryMap);
	}

	public void initCommander(
			final JCommander commander ) {
		for (final Entry<String, InternalOperationCategory> category : categoryMap.entrySet()) {
			final InternalOperationCategory internalCategory = category.getValue();
			final JCommander categoryCommander = addCommand(
					commander,
					category.getKey(),
					internalCategory.category);
			final Iterator<InternalOperation> opsIt = internalCategory.opsMap.values().iterator();
			while (opsIt.hasNext()) {
				final InternalOperation op = opsIt.next();
				addCommand(
						categoryCommander,
						op.name,
						op.op);
			}
		}
	}

	public boolean run(
			final JCommander commander ) {
		final String command = commander.getParsedCommand();
		final InternalOperationCategory category = categoryMap.get(
				command);
		final JCommander categoryCommander = commander.getCommands().get(
				command);
		if ((category == null) || (categoryCommander == null)) {
			JCommander.getConsole().println(
					"Unable to parse GeoWave command category '" + command + "'");
			return false;
		}
		if (category.category.isHelp()) {
			categoryCommander.usage();
			return true;
		}
		final String categoryCommand = categoryCommander.getParsedCommand();
		final InternalOperation operation = category.opsMap.get(
				categoryCommand);
		final JCommander operationCommander = commander.getCommands().get(
				categoryCommand);
		if ((operation == null) || (operationCommander == null)) {
			JCommander.getConsole().println(
					"Unable to parse GeoWave command operation '" + command + "'");
			return false;
		}
		if (operation.op.isHelp()) {
			operationCommander.usage();
			return true;
		}
		return operation.op.doOperation(
				operationCommander);
	}

	private static JCommander addCommand(
			final JCommander parentCommand,
			final String commandName,
			final CommandObject command ) {
		parentCommand.addCommand(
				commandName,
				command);
		final JCommander commander = parentCommand.getCommands().get(
				commandName);
		command.init(
				commander);
		return commander;
	}

	private static class InternalOperationCategory
	{
		private final String name;
		private final CommandObject category;
		private Map<String, InternalOperation> opsMap;

		public InternalOperationCategory(
				final CommandObject category ) {
			super();
			this.category = category;
			name = category.getName();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((name == null) ? 0 : name.hashCode());
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
			final InternalOperationCategory other = (InternalOperationCategory) obj;
			if (name == null) {
				if (other.name != null) {
					return false;
				}
			}
			else if (!name.equals(
					other.name)) {
				return false;
			}
			return true;
		}
	}

	private static class InternalOperation
	{
		private final String name;
		private final CLIOperation op;

		public InternalOperation(
				final CLIOperation op ) {
			super();
			this.op = op;
			name = op.getName();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((name == null) ? 0 : name.hashCode());
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
			final InternalOperation other = (InternalOperation) obj;
			if (name == null) {
				if (other.name != null) {
					return false;
				}
			}
			else if (!name.equals(
					other.name)) {
				return false;
			}
			return true;
		}
	}
}
