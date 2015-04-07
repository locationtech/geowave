package mil.nga.giat.geowave.analytics.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.index.PersistenceUtils;
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
 * The class supports some basic conversions.
 * 
 * Non-serializable objects: {@link Persistable} instances are converted to and
 * from byte formats. {@link DistributableQuery} is a special case, supporting
 * WKT String. {@link Path} are converted to a from string representation of the
 * their URI.
 * 
 * Serializable objects: {@link NumericRange} supports min,max in string
 * representation (e.g. "1.0,2.0")
 * 
 * 
 * NOTE: ConfigutationWrapper implementation is scopeless.
 * 
 * EXPECTED FUTURE WORK: I am bit unsatisfied with the duality of the parameters
 * base class. In one case, in is treated a description for a class value and,
 * in the other case, it is treated as a description for the type of a property
 * value. The former is really a descriptor of a Class of type class. Generics
 * do not help due to erasure. The impact of this inconsistency is the inability
 * to validate on 'store'. Instead, validation occurs on 'gets'. The ultimate
 * goal is to uniformly provide feedback to parameters from command line
 * arguments and property files on submission to the manager rather than on
 * extraction from the manager.
 * 
 */
public class PropertyManagement implements
		ConfigurationWrapper,
		Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4186468044516636362L;
	final static Logger LOGGER = LoggerFactory.getLogger(PropertyManagement.class);

	private final HashMap<String, Serializable> properties = new HashMap<String, Serializable>();
	private final ArrayList<PropertyConverter<?>> converters = new ArrayList<PropertyConverter<?>>();

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
			final ParameterEnum param ) {
		return param.getClass().getSimpleName().toLowerCase() + "-" + param.self().name().replace(
				'_',
				'-').toLowerCase();
	}

	public PropertyManagement() {
		converters.add(new QueryConverter());
		converters.add(new PathConverter());
		converters.add(new PersistableConverter());
	}

	public PropertyManagement(
			final PropertyConverter<?>[] converters,
			final ParameterEnum[] names,
			final Object[] values ) {
		this.converters.add(new QueryConverter());
		this.converters.add(new PathConverter());
		this.converters.add(new PersistableConverter());
		for (PropertyConverter<?> converter : converters)
			this.addConverter(converter);
		store(
				names,
				values);
	}

	public PropertyManagement(
			final ParameterEnum[] names,
			final Object[] values ) {
		converters.add(new QueryConverter());
		converters.add(new PathConverter());
		converters.add(new PersistableConverter());
		store(
				names,
				values);
	}

	public Serializable get(
			final ParameterEnum propertyName ) {
		return properties.get(toPropertyName(propertyName));
	}

	public synchronized <T> void store(
			final ParameterEnum property,
			final T value,
			final PropertyConverter<T> converter ) {
		Serializable convertedValue;
		try {
			convertedValue = converter.convert(value);
		}
		catch (Exception e) {
			throw new IllegalArgumentException(
					String.format(
							"Cannot store %s with value %s. Expected type = %s; Error message = %s",
							toPropertyName(property),
							value.toString(),
							property.getBaseClass().toString(),
							e.getLocalizedMessage()));
		}
		properties.put(
				toPropertyName(property),
				convertedValue);
		this.addConverter(converter);
	}

	public synchronized void store(
			final ParameterEnum property,
			final Object value ) {
		Serializable convertedValue;
		try {
			convertedValue = convertIfNecessary(
					property,
					value);
		}
		catch (Exception e) {
			throw new IllegalArgumentException(
					String.format(
							"Cannot store %s with value %s:%s",
							toPropertyName(property),
							value.toString(),
							e.getLocalizedMessage()));
		}
		properties.put(
				toPropertyName(property),
				convertedValue);
	}

	/**
	 * Does not work for non-serializable data (e.g. Path or Persistable)
	 * 
	 */

	public synchronized Serializable storeIfEmpty(
			final ParameterEnum propertyName,
			final Serializable value ) {
		final String pName = toPropertyName(propertyName);
		if (!properties.containsKey(pName)) {
			LOGGER.info(
					"Setting parameter : {} to {}",
					pName,
					value.toString());
			store(
					propertyName,
					value);
			return value;
		}
		return properties.get(pName);
	}

	public synchronized void copy(
			final ParameterEnum propertyNameFrom,
			final ParameterEnum propertyNameTo ) {
		if (properties.containsKey(toPropertyName(propertyNameFrom))) {
			properties.put(
					toPropertyName(propertyNameTo),
					properties.get(toPropertyName(propertyNameFrom)));
		}
	}

	public synchronized void store(
			final ParameterEnum[] names,
			final Object[] values ) {
		if (values.length != names.length) {
			LOGGER.error("The number of values must equal the number of names passed to the store method");
			throw new IllegalArgumentException(
					"The number of values must equal the number of names passed to the store method");
		}
		int i = 0;
		for (final Object value : values) {
			store(
					names[i++],
					value);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T getClassInstance(
			final ParameterEnum property,
			final Class<T> defaultClass )
			throws InstantiationException {
		final Object o = properties.get(toPropertyName(property));

		try {
			final Class<?> clazz = o == null ? defaultClass : (o instanceof Class) ? (Class<?>) o : Class.forName(o.toString());
			if (!property.getBaseClass().isAssignableFrom(
					clazz)) {
				LOGGER.error("Class for property " + toPropertyName(property) + " does not implement " + property.getBaseClass().toString());
			}
			return (T) clazz.newInstance();
		}
		catch (final ClassNotFoundException e) {
			LOGGER.error(
					"Class for property " + toPropertyName(property) + " is not found",
					e);
			throw new InstantiationException(
					toPropertyName(property));
		}
		catch (final InstantiationException e) {
			LOGGER.error(
					"Class for property " + toPropertyName(property) + " is not instiatable",
					e);
			throw new InstantiationException(
					toPropertyName(property));
		}
		catch (final IllegalAccessException e) {
			LOGGER.error(
					"Class for property " + toPropertyName(property) + " is not accessible",
					e);
			throw new InstantiationException(
					toPropertyName(property));
		}
	}

	public synchronized boolean hasProperty(
			final ParameterEnum property ) {
		return properties.containsKey(toPropertyName(property));
	}

	public String getPropertyAsString(
			final ParameterEnum property ) {
		return getPropertyAsString(
				property,
				null);
	}

	/**
	 * Returns the value as, without conversion from the properties. Throws an
	 * exception if a conversion is required to a specific type
	 * 
	 * @param property
	 * @return
	 * @throws Exception
	 * @throws IllegalArgumentException
	 */
	public Object getProperty(
			final ParameterEnum property )
			throws Exception {

		final Serializable value = properties.get(toPropertyName(property));
		if (!Serializable.class.isAssignableFrom(property.getBaseClass())) {
			for (PropertyConverter converter : this.converters)
				if (property.getBaseClass().isAssignableFrom(
						converter.baseClass())) {
					return this.validate(
							property,
							converter.convert(value));
				}
		}
		return (Serializable) this.validate(
				property,
				value);
	}

	/**
	 * Returns the value after conversion. Throws an exception if a conversion
	 * fails.
	 * 
	 * @param property
	 * @return
	 * @throws Exception
	 * @throws IllegalArgumentException
	 */
	public <T> T getProperty(
			final ParameterEnum property,
			PropertyConverter<T> converter )
			throws Exception {

		final Serializable value = properties.get(toPropertyName(property));
		return converter.convert(value);
	}

	public byte[] getPropertyAsBytes(
			final ParameterEnum property ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof byte[]) {
				return (byte[]) val;
			}
			return ByteArrayUtils.byteArrayFromString(val.toString());
		}
		return null;
	}

	public String getPropertyAsString(
			final ParameterEnum property,
			final String defaultValue ) {
		final String name = toPropertyName(property);
		// not using containsKey to avoid synchronization
		final Object value = properties.get(name);
		return (String) validate(
				property,
				value == null ? defaultValue : value.toString());
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
			if (val instanceof Integer) return (Integer) val;
			return (Integer) validate(
					property,
					Integer.parseInt(val.toString()));
		}
		LOGGER.warn("Using default value for parameter : " + toPropertyName(property));
		return defaultValue;
	}

	public Double getPropertyAsDouble(
			final ParameterEnum property,
			final double defaultValue ) {
		final Object val = properties.get(toPropertyName(property));
		if (val != null) {
			if (val instanceof Double) return (Double) val;
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
				throw new IllegalArgumentException(
						"Invalid class for property" + property);
			}
		}
		return null;
	}

	public <T> Class<T> getPropertyAsClass(
			final ParameterEnum property,
			final Class<T> iface )
			throws ClassNotFoundException {
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
				LOGGER.error("Class not found for property " + toPropertyName(property));
				throw e;
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.error(
						"Invalid class for property" + toPropertyName(property),
						ex);
				throw new IllegalArgumentException(
						"Invalid class for property" + property);
			}
		}
		else {
			LOGGER.error("Value not found for property " + toPropertyName(property));
		}
		throw new ClassNotFoundException(
				"Value not found for property " + toPropertyName(property));
	}

	public <T> Class<? extends T> getPropertyAsClass(
			final ParameterEnum property,
			final Class<? extends T> iface,
			final Class<? extends T> defaultClass ) {
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
				throw new IllegalArgumentException(
						"Invalid class for property" + property);
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
		final Serializable val = properties.get(toPropertyName(property));
		if (val != null) {
			return (DistributableQuery) validate(
					property,
					new QueryConverter().convert(val));
		}
		return null;
	}

	public Path getPropertyAsPath(
			final ParameterEnum property )
			throws Exception {
		final Serializable val = properties.get(toPropertyName(property));
		if (val != null) {
			return (Path) validate(
					property,
					new PathConverter().convert(val));
		}
		return null;
	}

	public Persistable getPropertyAsPersistable(
			final ParameterEnum property )
			throws Exception {

		final Serializable val = properties.get(toPropertyName(property));
		if (val != null) {
			return (Persistable) validate(
					property,
					new PersistableConverter().convert(val));
		}
		return null;
	}

	public synchronized String[] toArguments(
			final ParameterEnum[] names ) {
		final String[] resultArgs = new String[names.length];
		int i = 0;
		for (final ParameterEnum name : names) {
			resultArgs[i] = getPropertyAsString(
					name,
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

	/**
	 * Does not validate the option values.
	 * 
	 * @param commandLine
	 * @throws ParseException
	 */
	public synchronized void buildFromOptions(
			final CommandLine commandLine )
			throws ParseException {
		for (final Option option : commandLine.getOptions()) {
			if (!option.hasArg()) {
				properties.put(
						option.getLongOpt(),
						Boolean.TRUE);
			}
			else {
				properties.put(
						option.getLongOpt(),
						option.getValue());
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

	@Override
	public int getInt(
			Enum<?> property,
			Class<?> scope,
			int defaultValue ) {
		return this.getPropertyAsInt(
				(ParameterEnum) (property),
				defaultValue);
	}

	@Override
	public double getDouble(
			Enum<?> property,
			Class<?> scope,
			double defaultValue ) {
		return this.getPropertyAsDouble(
				(ParameterEnum) (property),
				defaultValue);
	}

	@Override
	public String getString(
			Enum<?> property,
			Class<?> scope,
			String defaultValue ) {
		return this.getPropertyAsString(
				(ParameterEnum) (property),
				defaultValue);
	}

	@Override
	public <T> T getInstance(
			Enum<?> property,
			Class<?> scope,
			Class<T> iface,
			Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		return this.getPropertyAsClass(
				(ParameterEnum) (property),
				iface,
				defaultValue).newInstance();
	}

	@Override
	public byte[] getBytes(
			Enum<?> property,
			Class<?> scope ) {
		return getPropertyAsBytes((ParameterEnum) property);
	}

	public void toOutput(
			OutputStream os )
			throws IOException {
		try (ObjectOutputStream oos = new ObjectOutputStream(
				os)) {
			oos.writeObject(properties);
		}
	}

	public void fromInput(
			InputStream is )
			throws IOException,
			ClassNotFoundException {
		try (ObjectInputStream ois = new ObjectInputStream(
				is)) {
			properties.clear();
			properties.putAll((HashMap<String, Serializable>) ois.readObject());
		}
	}

	public void fromProperties(
			Properties properties ) {
		this.properties.clear();
		for (Object key : properties.keySet()) {
			this.properties.put(
					key.toString(),
					properties.getProperty(key.toString()));
		}
	}

	/**
	 * Add to the set of converters used to take a String representation of a
	 * value and convert it into another serializable form.
	 * 
	 * This is done if the preferred internal representation does not match that
	 * of a string. For example, a query is maintained as bytes even though it
	 * can be provided as a query
	 * 
	 * @param converter
	 */
	public synchronized void addConverter(
			PropertyConverter<?> converter ) {
		this.converters.add(converter);
	}

	private static byte[] toBytes(
			Persistable persistableObject )
			throws UnsupportedEncodingException {
		return PersistenceUtils.toBinary(persistableObject);
	}

	private static Persistable fromBytes(
			byte[] data,
			Class<? extends Persistable> expectedType )
			throws InstantiationException,
			IllegalAccessException,
			ClassNotFoundException,
			UnsupportedEncodingException {
		return PersistenceUtils.fromBinary(
				data,
				expectedType);
	}

	private Object validate(
			final ParameterEnum propertyName,
			final Object value ) {
		if (value != null) {
			if (value instanceof Class) {
				if (propertyName.getBaseClass().isAssignableFrom(
						(Class<?>) value)) throw new IllegalArgumentException(
						String.format(
								"%s does not accept class %s",
								toPropertyName(propertyName),
								((Class<?>) value).getName()));
			}
			else if (!propertyName.getBaseClass().isInstance(
					value)) throw new IllegalArgumentException(
					String.format(
							"%s does not accept type %s",
							toPropertyName(propertyName),
							value.getClass().getName()));
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private Serializable convertIfNecessary(
			ParameterEnum property,
			final Object value )
			throws Exception {

		if (!(value instanceof Serializable)) {
			for (@SuppressWarnings("rawtypes")
			PropertyConverter converter : converters) {
				if (property.getBaseClass().isAssignableFrom(
						converter.baseClass())) {
					return converter.convert(value);
				}
			}
		}
		if (!property.getBaseClass().isInstance(
				value) && value instanceof String) {
			for (@SuppressWarnings("rawtypes")
			PropertyConverter converter : converters) {
				if (property.getBaseClass().isAssignableFrom(
						converter.baseClass())) {
					return converter.convert(converter.convert(value.toString()));
				}
			}
		}
		return (Serializable) value;
	}

	public interface PropertyConverter<T> extends
			Serializable
	{
		public Serializable convert(
				T ob )
				throws Exception;

		public T convert(
				Serializable ob )
				throws Exception;

		public Class<T> baseClass();
	}

	public static class QueryConverter implements
			PropertyConverter<DistributableQuery>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				DistributableQuery ob ) {
			try {
				return toBytes(ob);
			}
			catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException(
						String.format(
								"Cannot convert %s to a DistributableQuery: %s",
								ob.toString(),
								e.getLocalizedMessage()));
			}
		}

		@Override
		public DistributableQuery convert(
				Serializable ob )
				throws Exception {
			if (ob instanceof byte[]) {
				return (DistributableQuery) PropertyManagement.fromBytes(
						(byte[]) ob,
						DistributableQuery.class);
			}
			final PrecisionModel precision = new PrecisionModel();
			final GeometryFactory geometryFactory = new GeometryFactory(
					precision,
					4326);
			final WKTReader wktReader = new WKTReader(
					geometryFactory);
			return new SpatialQuery(
					wktReader.read(ob.toString()));
		}

		@Override
		public Class<DistributableQuery> baseClass() {
			return DistributableQuery.class;
		}
	}

	public static class PathConverter implements
			PropertyConverter<Path>
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				Path ob ) {
			return ob.toUri().toString();
		}

		@Override
		public Path convert(
				Serializable ob )
				throws Exception {
			return new Path(
					ob.toString());
		}

		@Override
		public Class<Path> baseClass() {
			return Path.class;
		}
	}

	public static class PersistableConverter implements
			PropertyConverter<Persistable>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				Persistable ob ) {
			try {
				return toBytes(ob);
			}
			catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException(
						String.format(
								"Cannot convert %s to a Persistable: %s",
								ob.toString(),
								e.getLocalizedMessage()));
			}
		}

		@Override
		public Persistable convert(
				Serializable ob )
				throws Exception {
			if (ob instanceof byte[]) {
				return fromBytes(
						(byte[]) ob,
						Persistable.class);
			}
			throw new IllegalArgumentException(
					String.format(
							"Cannot convert %s to Persistable",
							ob.toString()));
		}

		@Override
		public Class<Persistable> baseClass() {
			return Persistable.class;
		}

	}
}
