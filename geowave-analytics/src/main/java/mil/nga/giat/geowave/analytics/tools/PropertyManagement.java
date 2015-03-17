package mil.nga.giat.geowave.analytics.tools;

import java.util.Properties;
import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Manage properties used by the Map Reduce environment that are provided
 * through the API (e.g. command). Allow these arguments to be placed an 'args'
 * list for 'main' executables (e.g. ToolRunner).
 * 
 */
public class PropertyManagement
{
	final static Logger LOGGER = LoggerFactory.getLogger(PropertyManagement.class);
	private final Object MUTEX = new Object();

	private final Properties properties = new Properties();

	public static final Option newOption(
			final ParameterEnum e,
			final String shortCut,
			final String description,
			final boolean hasArg ) {
		return new Option(
				shortCut,
				toPropertyName(e),
				hasArg,
				description);
	}

	private static final String toPropertyName(
			final ParameterEnum e ) {
		return e.getClass().getSimpleName().toLowerCase() + "-" + e.self().name().replace(
				'_',
				'-').toLowerCase();
	}

	public PropertyManagement() {}

	public PropertyManagement(
			final ParameterEnum[] names,
			final Object[] values ) {
		store(
				names,
				values);
	}

	public Object get(
			final ParameterEnum propertyName ) {
			return properties.get(toPropertyName(propertyName));
	}

	public void store(
			final ParameterEnum propertyName,
			final Object value ) {
		synchronized (MUTEX) {
			properties.put(
					toPropertyName(propertyName), value);
		}
	}

	public Object storeIfEmpty(
			final ParameterEnum propertyName,
			final Object value ) {
		final String pName = toPropertyName(propertyName);
		synchronized (MUTEX) {
			if (!properties.containsKey(pName)) {
				LOGGER.info("Setting parameter : " + pName + " to " + value.toString());
				properties.put(
						pName, value);
				return value;
			}

			return properties.get(pName);
		}
	}


	public synchronized void copy(
			final ParameterEnum propertyNameFrom,
			final ParameterEnum propertyNameTo ) {
		synchronized (MUTEX) {
			if (properties.containsKey(toPropertyName(propertyNameFrom))) {
				properties.put(
						toPropertyName(propertyNameTo),
						properties.get(toPropertyName(propertyNameFrom)));
			}
		}
	}

	public void store(
			final ParameterEnum[] names,
			final Object[] values ) {
		if (values.length != names.length){
			LOGGER.error("The number of values must equal the number of names passed to the store method");
			throw new IllegalArgumentException("The number of values must equal the number of names passed to the store method");
		}
		synchronized (MUTEX) {
			int i = 0;
			for (final Object value : values) {
				properties.put(
						toPropertyName(names[i++]), value);
			}
		}
	}

	public Object getClassInstance(
			final ParameterEnum property ) {
		final Object o = properties.get(toPropertyName(property));
		if (o != null) {
			try {
				final Class<?> clazz = (o instanceof Class) ? (Class<?>) o : Class.forName(o.toString());
				if (!property.getBaseClass().isAssignableFrom(
						clazz)) {
					LOGGER.error("Class for property " + toPropertyName(property) + " does not implement " + property.getBaseClass().toString());
				}
				return clazz.newInstance();
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error(
						"Class for property " + toPropertyName(property) + " is not found",
						e);
			}
			catch (final InstantiationException e) {
				LOGGER.error(
						"Class for property " + toPropertyName(property) + " is not instiatable",
						e);
			}
			catch (final IllegalAccessException e) {
				LOGGER.error(
						"Class for property " + toPropertyName(property) + " is not accessible",
						e);
			}
		}
		return null;
	}

	public boolean hasProperty(
			final ParameterEnum property ) {
		return properties.getProperty(toPropertyName(property)) != null;
	}

	public String getProperty(
			final ParameterEnum property ) {
		return properties.getProperty(toPropertyName(property));
	}

	public Path getPropertyAsPath(
			final ParameterEnum property ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof Path) {
				return (Path) val;
			}
			return new Path(
					val.toString());
		}
		return null;
	}

	public String getProperty(
			final ParameterEnum property,
			final String defaultValue ) {
		return properties.getProperty(
				toPropertyName(property),
				defaultValue);
	}

	public Boolean getPropertyAsBoolean(
			final ParameterEnum property,
			final Boolean defaultValue ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			return Boolean.valueOf(val.toString());
		}
		LOGGER.warn("Using default value for parameter : " + toPropertyName(property));
		return defaultValue;
	}

	public Integer getPropertyAsInt(
			final ParameterEnum property,
			final int defaultValue ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			return Integer.parseInt(val.toString());
		}
		LOGGER.warn("Using default value for parameter : " + toPropertyName(property));
		return defaultValue;
	}

	public Double getPropertyAsDouble(
			final ParameterEnum property,
			final double defaultValue ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			return Double.parseDouble(val.toString());
		}
		LOGGER.warn("Using default value for parameter : " + toPropertyName(property));
		return defaultValue;
	}

	public NumericRange getPropertyAsRange(
			final ParameterEnum property,
			final NumericRange defaultValue ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof NumericRange) {
				return (NumericRange) val;
			}
			final String p = val.toString();
			final String[] parts = p.split(",");
			try {
				if (parts.length == 2) {
					return new NumericRange(
							Double.parseDouble(parts[0].trim()),
							Double.parseDouble(parts[1].trim()));
				}
				else {
					return new NumericRange(
							0,
							Double.parseDouble(p));
				}
			}
			catch (final Exception ex) {
				LOGGER.error("Invalid range parameter " + toPropertyName(property));
				return defaultValue;
			}
		}
		LOGGER.warn("Using default value for parameter : " + toPropertyName(property));
		return defaultValue;
	}

	public Class<?> getPropertyAsClass(
			final ParameterEnum property ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof Class) {
				return validate(
						(Class<?>) val,
						property.getBaseClass());
			}
			try {
				return validate(
						(Class<?>) Class.forName(val.toString()),
						property.getBaseClass());
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error("Class not found for property " + property);
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.error(
						"Invalid class for property" + property,
						ex);
			}
		}
		return null;
	}

	public <T> Class<T> getPropertyAsClass(
			final ParameterEnum property,
			final Class<T> iface ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof Class) {
				return validate(
						(Class<T>) val,
						property.getBaseClass());
			}
			try {
				return validate(
						(Class<T>) Class.forName(val.toString()),
						property.getBaseClass());
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error("Class not found for property " + property);
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.error(
						"Invalid class for property" + property,
						ex);
			}
		}
		return null;
	}

	public <T> Class<? extends T> getPropertyAsClass(
			final ParameterEnum property,
			final Class<? extends T> defaultClass,
			final Class<? extends T> iface ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof Class) {
				return validate(
						(Class<T>) val,
						property.getBaseClass());
			}
			try {
				return validate(
						(Class<T>) Class.forName(val.toString()),
						property.getBaseClass());
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error("Class not found for property " + property);
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.error(
						"Invalid class for property" + property,
						ex);
			}
		}
		LOGGER.warn("Using default class for parameter : " + toPropertyName(property));
		return defaultClass;
	}

	private <T> Class<T> validate(
			final Class<T> classToValidate,
			final Class<?> iface )
			throws IllegalArgumentException {
		if (!iface.isAssignableFrom(classToValidate)) {
			throw new IllegalArgumentException(
					classToValidate + "is an invalid subclass of " + iface);
		}
		return classToValidate;
	}

	public DistributableQuery getPropertyAsQuery(
			final ParameterEnum property )
			throws Exception {

		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof DistributableQuery) {
				return (DistributableQuery) val;
			}
			final PrecisionModel precision = new PrecisionModel();
			final GeometryFactory geometryFactory = new GeometryFactory(
					precision,
					4326);
			final WKTReader wktReader = new WKTReader(
					geometryFactory);
			return new SpatialQuery(
					wktReader.read(val.toString()));
		}
		return null;
	}

	public String[] toArguments(
			final ParameterEnum[] names ) {
		final String[] resultArgs = new String[names.length];
		int i = 0;
		for (final ParameterEnum name : names) {
			resultArgs[i] = properties.getProperty(
					toPropertyName(name),
					"");
			i++;
		}
		return resultArgs;
	}

	/**
	 * Arguments, in the correct order, passed to {@link GeoWaveJobRunner}
	 */
	public static final ParameterEnum[] GeoWaveRunnerArguments = new ParameterEnum[] {
		GlobalParameters.Global.ZOOKEEKER,
		GlobalParameters.Global.ACCUMULO_INSTANCE,
		GlobalParameters.Global.ACCUMULO_USER,
		GlobalParameters.Global.ACCUMULO_PASSWORD,
		GlobalParameters.Global.ACCUMULO_NAMESPACE
	};

	public String[] toGeoWaveRunnerArguments() {
		return toArguments(GeoWaveRunnerArguments);
	}

	public void buildFromOptions(
			final CommandLine commandLine )
			throws ParseException {
		synchronized (MUTEX) {
			for (final Option option : commandLine.getOptions()) {
				if (!option.hasArg()) {
					properties.put(
							option.getLongOpt(), true);
				}
				else {
					properties.put(
							option.getLongOpt(), option.getValue());
				}
			}
		}
	}

	public static void removeOption(
			final Set<Option> options,
			final ParameterEnum parameter ) {
		for (final Option option : options) {
			if (option.getLongOpt().equals(
					toPropertyName(parameter))) {
				options.remove(option);
				break;
			}
		}
	}

}
