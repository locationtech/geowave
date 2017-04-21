package mil.nga.giat.geowave.core.cli.spi;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.Operation;

/**
 * An operation entry represents an Operation Parsed from SPI, which is then
 * subsequently added to an OperationExecutor for execution.
 */
public final class OperationEntry
{
	private static Logger LOGGER = LoggerFactory.getLogger(OperationEntry.class);

	private final String operationName;
	private final Class<?> operationClass;
	private final Class<?> parentOperationClass;
	private final Map<String, OperationEntry> children;
	private final boolean command;
	private final boolean topLevel;

	/**
	 * Constructor from operation class
	 * 
	 * @param operationClass
	 *            operation class
	 */
	public OperationEntry(
			Class<?> operationClass ) {
		this.operationClass = operationClass;
		GeowaveOperation operation = this.operationClass.getAnnotation(GeowaveOperation.class);
		if (operation == null) {
			throw new RuntimeException(
					"Expected Operation class to use GeowaveOperation annotation: "
							+ this.operationClass.getCanonicalName());
		}
		this.operationName = operation.name();
		this.parentOperationClass = operation.parentOperation();
		this.command = Arrays.asList(
				this.operationClass.getInterfaces()).contains(
				Command.class);
		this.topLevel = this.parentOperationClass == null || this.parentOperationClass == Object.class;
		this.children = new HashMap<String, OperationEntry>();
	}

	/**
	 * Get the parent operation class
	 * 
	 * @return the parent operation class
	 */
	public Class<?> getParentOperationClass() {
		return parentOperationClass;
	}

	/**
	 * Get the operation name
	 * 
	 * @return the operation name
	 */
	public String getOperationName() {
		return operationName;
	}

	/**
	 * Get the operation class
	 * 
	 * @return the operation class
	 */
	public Class<?> getOperationClass() {
		return operationClass;
	}

	/**
	 * Get the child operations under this operation
	 * 
	 * @return the child operations under this operation
	 */
	public Collection<OperationEntry> getChildren() {
		return Collections.unmodifiableCollection(children.values());
	}

	/**
	 * Add a child operation
	 * 
	 * @param child
	 *            child operation to add
	 */
	public void addChild(
			OperationEntry child ) {
		if (children.containsKey(child.getOperationName().toLowerCase(
				Locale.ENGLISH))) {
			throw new RuntimeException(
					"Duplicate operation name: " + child.getOperationName() + " for "
							+ this.getOperationClass().getName());
		}
		children.put(
				child.getOperationName().toLowerCase(
						Locale.ENGLISH),
				child);
	}

	/**
	 * Get a specific child operation from a name to lookup
	 * 
	 * @param name
	 *            name of child operation to retrieve
	 * @return child operation associated with specified name
	 */
	public OperationEntry getChild(
			String name ) {
		return children.get(name);
	}

	/**
	 * Specifies if operation is a command
	 * 
	 * @return true if operation is a command, false otherwise
	 */
	public boolean isCommand() {
		return command;
	}

	/**
	 * Specifies if operation is a top-level command operation
	 * 
	 * @return true if operation is a top-level command operation, false
	 *         otherwise
	 */
	public boolean isTopLevel() {
		return topLevel;
	}

	/**
	 * Method to create a new instance of this operation class
	 * 
	 * @return new instance of this operation class
	 */
	public Operation createInstance() {
		try {
			return (Operation) this.operationClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			LOGGER.error(
					"Unable to create new instance",
					e);
			return null;
		}
	}
}
