package mil.nga.giat.geowave.core.cli.spi;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.Operation;

/**
 * An operation entry represents an Operation Parsed from SPI, which is then
 * subsequently added to an OperationExecutor for execution.
 */
public final class OperationEntry
{
	private final String operationName;
	private final Class<?> operationClass;
	private final Class<?> parentOperationClass;
	private final Map<String, OperationEntry> children;
	private final boolean command;
	private final boolean topLevel;

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

	public Class<?> getParentOperationClass() {
		return parentOperationClass;
	}

	public String getOperationName() {
		return operationName;
	}

	public Class<?> getOperationClass() {
		return operationClass;
	}

	public Collection<OperationEntry> getChildren() {
		return Collections.unmodifiableCollection(children.values());
	}

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

	public OperationEntry getChild(
			String name ) {
		return children.get(name);
	}

	public boolean isCommand() {
		return command;
	}

	public boolean isTopLevel() {
		return topLevel;
	}

	public Operation createInstance() {
		try {
			return (Operation) this.operationClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			return null;
		}
	}
}
