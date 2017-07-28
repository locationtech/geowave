/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
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
		Serializable
{

	/**
	 *
	 */
	private static final long serialVersionUID = -4186468044516636362L;
	final static Logger LOGGER = LoggerFactory.getLogger(PropertyManagement.class);

	private final Map<ParameterEnum<?>, Serializable> localProperties = new HashMap<ParameterEnum<?>, Serializable>();
	private final List<PropertyConverter<?>> converters = new ArrayList<PropertyConverter<?>>();
	private PropertyManagement nestProperties = null;

	public PropertyManagement() {
		converters.add(new QueryConverter());
		converters.add(new QueryOptionsConverter());
		converters.add(new PathConverter());
		converters.add(new PersistableConverter());
		converters.add(new DoubleConverter());
		converters.add(new IntegerConverter());
		converters.add(new ByteConverter());
	}

	public PropertyManagement(
			final PropertyConverter<?>[] converters,
			final ParameterEnum<?>[] names,
			final Object[] values ) {
		this.converters.add(new QueryConverter());
		this.converters.add(new QueryOptionsConverter());
		this.converters.add(new PathConverter());
		this.converters.add(new PersistableConverter());
		this.converters.add(new DoubleConverter());
		this.converters.add(new IntegerConverter());
		this.converters.add(new ByteConverter());
		for (final PropertyConverter<?> converter : converters) {
			addConverter(converter);
		}
		storeAll(
				names,
				values);
	}

	public PropertyManagement(
			final ParameterEnum<?>[] names,
			final Object[] values ) {
		converters.add(new QueryConverter());
		converters.add(new QueryOptionsConverter());
		converters.add(new PathConverter());
		converters.add(new PersistableConverter());
		converters.add(new DoubleConverter());
		converters.add(new IntegerConverter());
		converters.add(new ByteConverter());
		storeAll(
				names,
				values);
	}

	public PropertyManagement(
			final PropertyManagement pm ) {
		nestProperties = pm;
		converters.addAll(pm.converters);
	}

	public Serializable get(
			final ParameterEnum<?> propertyName ) {
		return getPropertyValue(propertyName);
	}

	public synchronized <T> void store(
			final ParameterEnum<?> property,
			final T value,
			final PropertyConverter<T> converter ) {
		Serializable convertedValue;
		try {
			convertedValue = converter.convert(value);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					String.format(
							"Cannot store %s with value %s. Expected type = %s; Error message = %s",
							property.self().toString(),
							value.toString(),
							property.getHelper().getBaseClass().toString(),
							e.getLocalizedMessage()),
					e);
		}
		localProperties.put(
				property,
				convertedValue);
		addConverter(converter);
	}

	public synchronized void store(
			final ParameterEnum<?> property,
			final Object value ) {
		if (value != null) {
			Serializable convertedValue;
			try {
				convertedValue = convertIfNecessary(
						property,
						value);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						String.format(
								"Cannot store %s with value %s:%s",
								property.self().toString(),
								value.toString(),
								e.getLocalizedMessage()));
			}
			localProperties.put(
					property,
					convertedValue);
		}
	}

	/**
	 * Does not work for non-serializable data (e.g. Path or Persistable)
	 * 
	 */

	public synchronized Serializable storeIfEmpty(
			final ParameterEnum<?> propertyEnum,
			final Serializable value ) {
		if (!containsPropertyValue(propertyEnum) && value != null) {
			LOGGER.info(
					"Setting parameter : {} to {}",
					propertyEnum.toString(),
					value.toString());
			store(
					propertyEnum,
					value);
			return value;
		}
		return getPropertyValue(propertyEnum);
	}

	public synchronized void copy(
			final ParameterEnum<?> propertyNameFrom,
			final ParameterEnum<?> propertyNameTo ) {
		if (containsPropertyValue(propertyNameFrom)) {
			localProperties.put(
					propertyNameTo,
					getPropertyValue(propertyNameFrom));
		}
	}

	public synchronized void storeAll(
			final ParameterEnum<?>[] names,
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

	public void setConfig(
			final ParameterEnum<?>[] parameters,
			final Configuration config,
			final Class<?> scope ) {
		for (final ParameterEnum param : parameters) {
			Object value;
			try {
				value = getProperty(param);
				param.getHelper().setValue(
						config,
						scope,
						value);

			}
			catch (final Exception e) {
				LOGGER.error(
						"Property " + param.self().toString() + " is not available",
						e);
				throw new IllegalArgumentException(
						"Property " + param.self().toString() + " is not available",
						e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T getClassInstance(
			final ParameterEnum<?> property,
			final Class<T> iface,
			final Class<?> defaultClass )
			throws InstantiationException {
		final Object o = getPropertyValue(property);

		try {
			final Class<?> clazz = o == null ? defaultClass : (o instanceof Class) ? (Class<?>) o : Class.forName(o
					.toString());
			if (!property.getHelper().getBaseClass().isAssignableFrom(
					clazz)) {
				LOGGER.error("Class for property " + property.self().toString() + " does not implement "
						+ property.getHelper().getBaseClass().toString());
			}
			return (T) clazz.newInstance();
		}
		catch (final ClassNotFoundException e) {
			LOGGER.error(
					"Class for property " + property.self().toString() + " is not found",
					e);
			throw new InstantiationException(
					property.self().toString());
		}
		catch (final InstantiationException e) {
			LOGGER.error(
					"Class for property " + property.self().toString() + " is not instiatable",
					e);
			throw new InstantiationException(
					property.self().toString());
		}
		catch (final IllegalAccessException e) {
			LOGGER.error(
					"Class for property " + property.self().toString() + " is not accessible",
					e);
			throw new InstantiationException(
					property.self().toString());
		}
	}

	public synchronized boolean hasProperty(
			final ParameterEnum<?> property ) {
		return containsPropertyValue(property);
	}

	public String getPropertyAsString(
			final ParameterEnum<?> property ) {
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
			final ParameterEnum<?> property )
			throws Exception {
		final Serializable value = getPropertyValue(property);
		if (!Serializable.class.isAssignableFrom(property.getHelper().getBaseClass())) {
			for (final PropertyConverter converter : converters) {
				if (converter.baseClass().isAssignableFrom(
						property.getHelper().getBaseClass())) {
					return this.validate(
							property,
							converter.convert(value));
				}
			}
		}
		return this.validate(
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
			final ParameterEnum<?> property,
			final PropertyConverter<T> converter )
			throws Exception {

		final Serializable value = getPropertyValue(property);
		return converter.convert(value);
	}

	public byte[] getPropertyAsBytes(
			final ParameterEnum<?> property ) {
		final Object val = getPropertyValue(property);
		if (val != null) {
			if (val instanceof byte[]) {
				return (byte[]) val;
			}
			return ByteArrayUtils.byteArrayFromString(val.toString());
		}
		return null;
	}

	public String getPropertyAsString(
			final ParameterEnum<?> property,
			final String defaultValue ) {
		// not using containsKey to avoid synchronization
		final Object value = getPropertyValue(property);
		return (String) validate(
				property,
				value == null ? defaultValue : value.toString());
	}

	public Boolean getPropertyAsBoolean(
			final ParameterEnum<?> property,
			final Boolean defaultValue ) {
		final Object val = getPropertyValue(property);
		if (val != null) {
			return Boolean.valueOf(val.toString());
		}
		LOGGER.warn("Using default value for parameter : " + property.self().toString());
		return defaultValue;
	}

	public Integer getPropertyAsInt(
			final ParameterEnum<?> property,
			final int defaultValue ) {
		final Object val = getPropertyValue(property);
		if (val != null) {
			if (val instanceof Integer) {
				return (Integer) val;
			}
			return (Integer) validate(
					property,
					Integer.parseInt(val.toString()));
		}
		LOGGER.warn("Using default value for parameter : " + property.self().toString());
		return defaultValue;
	}

	public Double getPropertyAsDouble(
			final ParameterEnum<?> property,
			final double defaultValue ) {
		final Object val = getPropertyValue(property);
		if (val != null) {
			if (val instanceof Double) {
				return (Double) val;
			}
			return Double.parseDouble(val.toString());
		}
		LOGGER.warn("Using default value for parameter : " + property.self().toString());
		return defaultValue;
	}

	public NumericRange getPropertyAsRange(
			final ParameterEnum<?> property,
			final NumericRange defaultValue ) {
		final Object val = getPropertyValue(property);
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
				LOGGER.error(
						"Invalid range parameter " + property.self().toString(),
						ex);
				return defaultValue;
			}
		}
		LOGGER.warn("Using default value for parameter : " + property.self().toString());
		return defaultValue;
	}

	public Class<?> getPropertyAsClass(
			final ParameterEnum<?> property ) {
		final Object val = getPropertyValue(property);
		if (val != null) {
			if (val instanceof Class) {
				return validate(
						(Class<?>) val,
						property.getHelper().getBaseClass());
			}
			try {
				return validate(
						(Class<?>) Class.forName(val.toString()),
						property.getHelper().getBaseClass());
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error(
						"Class not found for property " + property,
						e);
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
			final ParameterEnum<?> property,
			final Class<T> iface )
			throws ClassNotFoundException {
		final Object val = getPropertyValue(property);
		if (val != null) {
			if (val instanceof Class) {
				return validate(
						(Class<T>) val,
						property.getHelper().getBaseClass());
			}
			try {
				return validate(
						(Class<T>) Class.forName(val.toString()),
						property.getHelper().getBaseClass());
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error("Class not found for property " + property.self().toString());
				throw e;
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.error(
						"Invalid class for property" + property.self().toString(),
						ex);
				throw new IllegalArgumentException(
						"Invalid class for property" + property);
			}
		}
		else {
			LOGGER.error("Value not found for property " + property.self().toString());
		}
		throw new ClassNotFoundException(
				"Value not found for property " + property.self().toString());
	}

	public <T> Class<? extends T> getPropertyAsClass(
			final ParameterEnum<?> property,
			final Class<? extends T> iface,
			final Class<? extends T> defaultClass ) {
		final Object val = getPropertyValue(property);
		if (val != null) {
			if (val instanceof Class) {
				return validate(
						(Class<T>) val,
						property.getHelper().getBaseClass());
			}
			try {
				return validate(
						(Class<T>) Class.forName(val.toString()),
						property.getHelper().getBaseClass());
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error(
						"Class not found for property " + property,
						e);
			}
			catch (final java.lang.IllegalArgumentException ex) {
				LOGGER.error(
						"Invalid class for property" + property,
						ex);
				throw new IllegalArgumentException(
						"Invalid class for property" + property);
			}
		}
		LOGGER.warn("Using default class for parameter : " + property.self().toString());
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
			final ParameterEnum<?> property )
			throws Exception {
		final Serializable val = getPropertyValue(property);
		if (val != null) {
			return (DistributableQuery) validate(
					property,
					new QueryConverter().convert(val));
		}
		return null;
	}

	public QueryOptions getPropertyAsQueryOptions(
			final ParameterEnum property )
			throws Exception {
		final Serializable val = getPropertyValue(property);
		if (val != null) {
			return (QueryOptions) validate(
					property,
					new QueryOptionsConverter().convert(val));
		}
		return null;
	}

	public Path getPropertyAsPath(
			final ParameterEnum<?> property )
			throws Exception {
		final Serializable val = getPropertyValue(property);
		if (val != null) {
			return (Path) validate(
					property,
					new PathConverter().convert(val));
		}
		return null;
	}

	public Persistable getPropertyAsPersistable(
			final ParameterEnum<?> property )
			throws Exception {

		final Serializable val = getPropertyValue(property);
		if (val != null) {
			return (Persistable) validate(
					property,
					new PersistableConverter().convert(val));
		}
		return null;
	}

	public void setJobConfiguration(
			final Configuration configuration,
			final Class<?> scope ) {
		for (final ParameterEnum param : localProperties.keySet()) {
			param.getHelper().setValue(
					configuration,
					scope,
					param.getHelper().getValue(
							this));
		}
		if ((nestProperties != null) && !nestProperties.localProperties.isEmpty()) {
			nestProperties.setJobConfiguration(
					configuration,
					scope);
		}
	}

	public void dump() {
		LOGGER.info("Properties : ");
		for (final Map.Entry<ParameterEnum<?>, Serializable> prop : localProperties.entrySet()) {
			LOGGER.info(
					"{} = {}",
					prop.getKey(),
					prop.getValue());
		}
		nestProperties.dump();
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
			final PropertyConverter<?> converter ) {
		converters.add(converter);
	}

	private static byte[] toBytes(
			final Persistable persistableObject )
			throws UnsupportedEncodingException {
		return PersistenceUtils.toBinary(persistableObject);
	}

	private static Persistable fromBytes(
			final byte[] data )
			throws InstantiationException,
			IllegalAccessException,
			ClassNotFoundException,
			UnsupportedEncodingException {
		return PersistenceUtils.fromBinary(data);
	}

	private Object validate(
			final ParameterEnum propertyName,
			final Object value ) {
		if (value != null) {
			if (value instanceof Class) {
				if (((Class<?>) value).isAssignableFrom(propertyName.getHelper().getBaseClass())) {
					throw new IllegalArgumentException(
							String.format(
									"%s does not accept class %s",
									propertyName.self().toString(),
									((Class<?>) value).getName()));
				}
			}
			else if (!propertyName.getHelper().getBaseClass().isInstance(
					value)) {
				throw new IllegalArgumentException(
						String.format(
								"%s does not accept type %s",
								propertyName.self().toString(),
								value.getClass().getName()));
			}
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private Serializable convertIfNecessary(
			final ParameterEnum property,
			final Object value )
			throws Exception {

		if (!(value instanceof Serializable)) {
			for (@SuppressWarnings("rawtypes")
			final PropertyConverter converter : converters) {
				if (converter.baseClass().isAssignableFrom(
						property.getHelper().getBaseClass())) {
					return converter.convert(value);
				}
			}
		}
		if (!property.getHelper().getBaseClass().isInstance(
				value) && (value instanceof String)) {
			for (@SuppressWarnings("rawtypes")
			final PropertyConverter converter : converters) {
				if (converter.baseClass().isAssignableFrom(
						property.getHelper().getBaseClass())) {
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

	public interface PropertyGroup<T extends Serializable> extends
			Serializable
	{
		public T convert(
				CommandLine commandLine )
				throws ParseException;

		public ParameterEnum getParameter();
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
				final DistributableQuery ob ) {
			try {
				return toBytes(ob);
			}
			catch (final UnsupportedEncodingException e) {
				throw new IllegalArgumentException(
						String.format(
								"Cannot convert %s to a DistributableQuery: %s",
								ob.toString(),
								e.getLocalizedMessage()));
			}
		}

		@Override
		public DistributableQuery convert(
				final Serializable ob )
				throws Exception {
			if (ob instanceof byte[]) {
				return (DistributableQuery) PropertyManagement.fromBytes((byte[]) ob);
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

	public static class QueryOptionsConverter implements
			PropertyConverter<QueryOptions>
	{

		/**
 *
 */
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				final QueryOptions ob ) {
			try {
				return toBytes(ob);
			}
			catch (final UnsupportedEncodingException e) {
				throw new IllegalArgumentException(
						String.format(
								"Cannot convert %s to a QueryOptions: %s",
								ob.toString(),
								e.getLocalizedMessage()));
			}
		}

		@Override
		public QueryOptions convert(
				final Serializable ob )
				throws Exception {
			if (ob instanceof byte[]) {
				return (QueryOptions) PropertyManagement.fromBytes((byte[]) ob);
			}
			else if (ob instanceof QueryOptions) {
				return (QueryOptions) ob;
			}
			return new QueryOptions();
		}

		@Override
		public Class<QueryOptions> baseClass() {
			return QueryOptions.class;
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
				final Path ob ) {
			return ob.toUri().toString();
		}

		@Override
		public Path convert(
				final Serializable ob )
				throws Exception {
			return new Path(
					ob.toString());
		}

		@Override
		public Class<Path> baseClass() {
			return Path.class;
		}
	}

	public static class ByteConverter implements
			PropertyConverter<byte[]>
	{
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				final byte[] ob ) {
			return ByteArrayUtils.byteArrayToString(ob);
		}

		@Override
		public byte[] convert(
				final Serializable ob )
				throws Exception {
			return ByteArrayUtils.byteArrayFromString(ob.toString());
		}

		@Override
		public Class<byte[]> baseClass() {
			return byte[].class;
		}
	}

	public static class IntegerConverter implements
			PropertyConverter<Integer>
	{
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				final Integer ob ) {
			return ob;
		}

		@Override
		public Integer convert(
				final Serializable ob )
				throws Exception {
			return Integer.parseInt(ob.toString());
		}

		@Override
		public Class<Integer> baseClass() {
			return Integer.class;
		}
	}

	public static class DoubleConverter implements
			PropertyConverter<Double>
	{
		/**
 *
 */
		private static final long serialVersionUID = 1L;

		@Override
		public Serializable convert(
				final Double ob ) {
			return ob;
		}

		@Override
		public Double convert(
				final Serializable ob )
				throws Exception {
			return Double.parseDouble(ob.toString());
		}

		@Override
		public Class<Double> baseClass() {
			return Double.class;
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
				final Persistable ob ) {
			try {
				return toBytes(ob);
			}
			catch (final UnsupportedEncodingException e) {
				throw new IllegalArgumentException(
						String.format(
								"Cannot convert %s to a Persistable: %s",
								ob.toString(),
								e.getLocalizedMessage()));
			}
		}

		@Override
		public Persistable convert(
				final Serializable ob )
				throws Exception {
			if (ob instanceof byte[]) {
				return fromBytes((byte[]) ob);
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

	private boolean containsPropertyValue(
			final ParameterEnum<?> property ) {
		return ((nestProperties != null) && nestProperties.containsPropertyValue(property))
				|| localProperties.containsKey(property);
	}

	private Serializable getPropertyValue(
			final ParameterEnum<?> property ) {
		final Serializable val = localProperties != null ? localProperties.get(property) : null;
		if (val == null) {
			return nestProperties != null ? nestProperties.getPropertyValue(property) : null;
		}
		return val;
	}
}
