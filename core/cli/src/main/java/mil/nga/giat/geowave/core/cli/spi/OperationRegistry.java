package mil.nga.giat.geowave.core.cli.spi;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.cli.api.Operation;

/**
 * This implementation uses the SPI to load all Operations across the program,
 * including those exported by plugsin. It parses the entries and places them
 * into a cache.
 */
public class OperationRegistry
{
	private Map<Class<?>, OperationEntry> operationMapByClass = null;

	/**
	 * Singleton pattern allows us to create a version that can be used by the
	 * whole application.
	 */
	private static class OperationRegistryHolder
	{
		public static final OperationRegistry instance = new OperationRegistry();
	}

	/**
	 * But also allow the user to create their own if they want it to be
	 * sanitized.
	 */
	public OperationRegistry() {
		init();
	}

	/**
	 * Get instance of the Operation Registry Holder
	 * 
	 * @return instance of the Operation Registry Holder
	 */
	public static OperationRegistry getInstance() {
		return OperationRegistryHolder.instance;
	}

	/**
	 * Constructor from a list of service entries
	 * 
	 * @param entries
	 *            list of service entries
	 */
	public OperationRegistry(
			List<OperationEntry> entries ) {
		operationMapByClass = new HashMap<Class<?>, OperationEntry>();
		for (OperationEntry entry : entries) {
			operationMapByClass.put(
					entry.getOperationClass(),
					entry);
		}
	}

	/**
	 * Initialize each of the service entries map of classes
	 */
	private synchronized void init() {
		if (operationMapByClass == null) {
			operationMapByClass = new HashMap<Class<?>, OperationEntry>();
			// Load SPI elements
			final Iterator<CLIOperationProviderSpi> operationProviders = ServiceLoader.load(
					CLIOperationProviderSpi.class).iterator();
			while (operationProviders.hasNext()) {
				final CLIOperationProviderSpi operationProvider = operationProviders.next();
				for (Class<?> clz : operationProvider.getOperations()) {
					if (Operation.class.isAssignableFrom(clz)) {
						OperationEntry entry = new OperationEntry(
								clz);
						operationMapByClass.put(
								clz,
								entry);
					}
					else {
						throw new RuntimeException(
								"CLI operations must be assignable from Operation.class: " + clz.getCanonicalName());
					}
				}
			}

			// Build a hierarchy.
			for (OperationEntry entry : operationMapByClass.values()) {
				if (!entry.isTopLevel()) {
					OperationEntry parentEntry = operationMapByClass.get(entry.getParentOperationClass());
					if (parentEntry == null) {
						throw new RuntimeException(
								"Cannot find parent entry for " + entry.getOperationClass().getName());
					}
					if (parentEntry.isCommand()) {
						throw new RuntimeException(
								"Cannot have a command be a parent: " + entry.getClass().getCanonicalName());
					}
					parentEntry.addChild(entry);
				}
			}
		}
	}

	/**
	 * Allow the iteration and exploration of all operations by a caller.
	 * Because we like callers.
	 * 
	 * @return collection of all service entries
	 */
	public Collection<OperationEntry> getAllOperations() {
		return Collections.unmodifiableCollection(operationMapByClass.values());
	}

	/**
	 * Get the exported service entry by class name
	 * 
	 * @param operationClass
	 *            Class to return the service entry for
	 * @return If it exists, return the class associated with the service entry
	 *         specified. If it does not exist, return null.
	 */
	public OperationEntry getOperation(
			Class<?> operationClass ) {
		return operationMapByClass.get(operationClass);
	}
}
