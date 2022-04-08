/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class PropertiesUtils implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtils.class);

  public static Properties fromFile(final String propertyFilePath) {
    return fromFile(new File(propertyFilePath));
  }

  public static Properties fromFile(final File propsFile) {
    Properties properties = null;
    if ((propsFile != null) && propsFile.exists()) {
      properties = new Properties();
      try {
        // HP Fortify "Improper Resource Shutdown or Release" false
        // positive
        // FileInputStream is closed automatically below as a result of
        // isr.close();
        final InputStreamReader isr =
            new InputStreamReader(new FileInputStream(propsFile), "UTF-8");
        if (isr != null) {
          properties.load(isr);
          isr.close();
        }
      } catch (final FileNotFoundException fnfEx) {
        LOGGER.error(
            "Specified properties file was not found: [" + fnfEx.getLocalizedMessage() + "]",
            fnfEx);
      } catch (final IOException ioEx) {
        LOGGER.error(
            "Exception occurred loading specified properties file: ["
                + ioEx.getLocalizedMessage()
                + "]",
            ioEx);
      }
    }
    return properties;
  }

  /**
   * Interface for providing properties to the configuration object Allows for objects other than
   * Maps and Properties to be used as a source for settings
   */
  public static interface Getter extends Serializable {
    /**
     * @param name Name of setting to lookup
     * @return Property value or NULL if it does not exist
     */
    public Object get(String name);
  };

  /** The interface to obtain property values */
  private final Getter getter;

  /**
   * Constructs a properties map that wraps these properties
   *
   * @param properties Map of properties to wrap
   */
  @SuppressWarnings({"rawtypes"})
  public PropertiesUtils(final Map properties) {
    this(new Getter() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Object get(final String name) {
        return properties.get(name);
      }
    });
  }

  /**
   * Constructs a properties map that wraps these properties
   *
   * @param properties Map of properties to wrap
   */
  public PropertiesUtils(final Properties properties) {
    this(new Getter() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Object get(final String name) {
        return properties != null ? properties.get(name) : null;
      }
    });
  }

  /**
   * Constructs a properties map that wraps these properties
   *
   * @param getter Getter interface to properties to map
   */
  public PropertiesUtils(final Getter getter) {
    this.getter = getter;
  }

  /**
   * Returns if this property exists
   *
   * @param key Property key to lookup
   * @return True if this property key exists
   */
  public boolean exists(final String key) {
    return this.get(key, Object.class) != null;
  }

  /**
   * Gets a value from the property map
   *
   * @param name Property name
   * @param req Is this property required?
   * @return Value for property
   */
  private Object getPropertyValue(final String name, final boolean req)
      throws IllegalArgumentException {
    Object val = null;
    if (getter != null) {
      val = getter.get(name);
      // Treat empty strings as null
      if ((val != null) && (val instanceof String) && ((String) val).isEmpty()) {
        val = null;
      }
      // HP Fortify "Privacy Violation" false positive
      // The information in the Properties file is not private or
      // sensitive
      if ((val == null) && req) {
        throw new IllegalArgumentException("Missing required property: " + name);
      }
    }
    return val;
  }

  /**
   * Get a required value from the map - throws an IllegalArgumentException if the value does not
   * exist
   *
   * @param <X> Data type for the return value
   * @param name Property name
   * @param clazz Class for type X
   * @return Value from the property map
   * @throws IllegalArgumentException Thrown if no value is found
   */
  public final <X> X get(final String name, final Class<X> clazz) throws IllegalArgumentException {
    final Object val = getPropertyValue(name, true);
    return ValueConverter.convert(val, clazz);
  }

  /**
   * Get a required value from the map - returns the provided default value if the value is not
   * found
   *
   * @param <X> Data type for the return value
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @param clazz Class for type X
   * @return Value from the property map
   */
  public final <X> X get(final String name, final X def, final Class<X> clazz) {
    final Object val = getPropertyValue(name, false);
    return (val == null) ? def : (X) ValueConverter.convert(val, clazz);
  }

  // ************************************************************************
  // ************************************************************************
  // ************************************************************************
  // The following are all convience methods for get of various types
  // ************************************************************************
  // ************************************************************************
  // ************************************************************************

  /**
   * Return the property value as a string
   *
   * @param name Property name
   * @return Property value converted to a string
   * @throws IllegalArgumentException
   */
  public final String getString(final String name) throws IllegalArgumentException {
    return get(name, String.class);
  }

  /**
   * Return the property value as a string if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a string
   */
  public final String getString(final String name, final String def) {
    return get(name, def, String.class);
  }

  /**
   * Return the property value as an integer
   *
   * @param name Property name
   * @return Property value converted to an integer
   * @throws IllegalArgumentException
   */
  public final Integer getInt(final String name) throws IllegalArgumentException {
    return get(name, Integer.class);
  }

  /**
   * Return the property value as an integer if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an integer
   */
  public final Integer getInt(final String name, final Integer def) {
    return get(name, def, Integer.class);
  }

  /**
   * Return the property value as a long
   *
   * @param name Property name
   * @return Property value converted to a long
   * @throws IllegalArgumentException
   */
  public final Long getLong(final String name) throws IllegalArgumentException {
    return get(name, Long.class);
  }

  /**
   * Return the property value as a long if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a long
   */
  public final Long getLong(final String name, final Long def) {
    return get(name, def, Long.class);
  }

  /**
   * Return the property value as a float
   *
   * @param name Property name
   * @return Property value converted to a float
   * @throws IllegalArgumentException
   */
  public final Float getFloat(final String name) throws IllegalArgumentException {
    return get(name, Float.class);
  }

  /**
   * Return the property value as a float if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a float
   */
  public final Float getFloat(final String name, final Float def) {
    return get(name, def, Float.class);
  }

  /**
   * Return the property value as a double
   *
   * @param name Property name
   * @return Property value converted to a double
   * @throws IllegalArgumentException
   */
  public final Double getDouble(final String name) throws IllegalArgumentException {
    return get(name, Double.class);
  }

  /**
   * Return the property value as a double if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a double
   */
  public final Double getDouble(final String name, final Double def) {
    return get(name, def, Double.class);
  }

  /**
   * Return the property value as a BigInteger
   *
   * @param name Property name
   * @return Property value converted to a BigInteger
   * @throws IllegalArgumentException
   */
  public final BigInteger getBigInteger(final String name) throws IllegalArgumentException {
    return get(name, BigInteger.class);
  }

  /**
   * Return the property value as a BigInteger if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a big integer
   */
  public final BigInteger getBigInteger(final String name, final BigInteger def) {
    return get(name, def, BigInteger.class);
  }

  /**
   * Return the property value as a BigDecimal
   *
   * @param name Property name
   * @return Property value converted to a big decimal
   * @throws IllegalArgumentException
   */
  public final BigDecimal getBigDecimal(final String name) throws IllegalArgumentException {
    return get(name, BigDecimal.class);
  }

  /**
   * Return the property value as a BigDecimal if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a big decimal
   */
  public final BigDecimal getBigDecimal(final String name, final BigDecimal def) {
    return get(name, def, BigDecimal.class);
  }

  /**
   * Return the property value as a binary
   *
   * @param name Property name
   * @return Property value converted to binary
   * @throws IllegalArgumentException
   */
  public final Byte getByte(final String name) throws IllegalArgumentException {
    return get(name, Byte.class);
  }

  /**
   * Return the property value as a binary if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to binary
   */
  public final Byte getByte(final String name, final Byte def) {
    return get(name, def, Byte.class);
  }

  /**
   * Return the property value as a boolean
   *
   * @param name Property name
   * @return Property value converted to a boolean
   * @throws IllegalArgumentException
   */
  public final Boolean getBoolean(final String name) throws IllegalArgumentException {
    return get(name, Boolean.class);
  }

  /**
   * Return the property value as a boolean if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a boolean
   */
  public final Boolean getBoolean(final String name, final Boolean def) {
    return get(name, def, Boolean.class);
  }

  /**
   * Return the property value as a URI
   *
   * @param name Property name
   * @return Property value converted to a URI
   * @throws IllegalArgumentException
   */
  public final URI getURI(final String name) throws IllegalArgumentException {
    return get(name, URI.class);
  }

  /**
   * Return the property value as a URI if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a URI
   */
  public final URI getURI(final String name, final URI def) {
    return get(name, def, URI.class);
  }

  /**
   * Return the property value as a URL
   *
   * @param name Property name
   * @return Property value converted to a URL
   * @throws IllegalArgumentException
   */
  public final URL getURL(final String name) throws IllegalArgumentException {
    return get(name, URL.class);
  }

  /**
   * Return the property value as a URL if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to a URL
   */
  public final URL getURI(final String name, final URL def) {
    return get(name, def, URL.class);
  }

  /**
   * Return the property value as a string array
   *
   * @param name Property name
   * @return Property value converted to an array of strings
   * @throws IllegalArgumentException
   */
  public final String[] getStringArray(final String name) throws IllegalArgumentException {
    return get(name, String[].class);
  }

  /**
   * Return the property value as a string array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of strings
   */
  public final String[] getStringArray(final String name, final String[] def) {
    return get(name, def, String[].class);
  }

  /**
   * Return the property value as an integer array
   *
   * @param name Property name
   * @return Property value converted to an array of integers
   * @throws IllegalArgumentException
   */
  public final Integer[] getIntArray(final String name) throws IllegalArgumentException {
    return get(name, Integer[].class);
  }

  /**
   * Return the property value as an integer array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of integers
   */
  public final Integer[] getIntArray(final String name, final Integer[] def) {
    return get(name, def, Integer[].class);
  }

  /**
   * Return the property value as a long array
   *
   * @param name Property name
   * @return Property value converted to an array of long values
   * @throws IllegalArgumentException
   */
  public final Long[] getLongArray(final String name) throws IllegalArgumentException {
    return get(name, Long[].class);
  }

  /**
   * Return the property value as a long array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of long values
   */
  public final Long[] getLongArray(final String name, final Long[] def) {
    return get(name, def, Long[].class);
  }

  /**
   * Return the property value as a float array
   *
   * @param name Property name
   * @return Property value converted to an array of float values
   * @throws IllegalArgumentException
   */
  public final Float[] getFloatArray(final String name) throws IllegalArgumentException {
    return get(name, Float[].class);
  }

  /**
   * Return the property value as a float array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of float values
   */
  public final Float[] getFloatArray(final String name, final Float[] def) {
    return get(name, def, Float[].class);
  }

  /**
   * Return the property value as a double array
   *
   * @param name Property name
   * @return Property value converted to an array of double values
   * @throws IllegalArgumentException
   */
  public final Double[] getDoubleArray(final String name) throws IllegalArgumentException {
    return get(name, Double[].class);
  }

  /**
   * Return the property value as a double array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of double values
   */
  public final Double[] getDoubleArray(final String name, final Double[] def) {
    return get(name, def, Double[].class);
  }

  /**
   * Return the property value as a BigInteger array
   *
   * @param name Property name
   * @return Property value converted to an array of big integers
   * @throws IllegalArgumentException
   */
  public final BigInteger[] getBigIntegerArray(final String name) throws IllegalArgumentException {
    return get(name, BigInteger[].class);
  }

  /**
   * Return the property value as a BigInteger array if it exists, otherwise return the default
   * value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of big integers
   */
  public final BigInteger[] getBigIntegerArray(final String name, final BigInteger[] def) {
    return get(name, def, BigInteger[].class);
  }

  /**
   * Return the property value as a BigDecimal array
   *
   * @param name Property name
   * @return Property value converted to an array of big decimals
   * @throws IllegalArgumentException
   */
  public final BigDecimal[] getBigDecimalArray(final String name) throws IllegalArgumentException {
    return get(name, BigDecimal[].class);
  }

  /**
   * Return the property value as a BigDecimal array if it exists, otherwise return the default
   * value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of big decimals
   */
  public final BigDecimal[] getBigDecimalArray(final String name, final BigDecimal[] def) {
    return get(name, def, BigDecimal[].class);
  }

  /**
   * Return the property value as a URI array
   *
   * @param name Property name
   * @return Property value converted to an array of URI's
   * @throws IllegalArgumentException
   */
  public final URI[] getURIArray(final String name) throws IllegalArgumentException {
    return get(name, URI[].class);
  }

  /**
   * Return the property value as a URI array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of URI's
   */
  public final URI[] getURIArray(final String name, final URI[] def) {
    return get(name, def, URI[].class);
  }

  /**
   * Return the property value as a URI array
   *
   * @param name Property name
   * @return Property value converted to an array of URI's
   * @throws IllegalArgumentException
   */
  public final URI[] getURLArray(final String name) throws IllegalArgumentException {
    return get(name, URI[].class);
  }

  /**
   * Return the property value as a URI array if it exists, otherwise return the default value
   *
   * @param name Property name
   * @param def Default value to return if the map does not include the value
   * @return Property value converted to an array of URI's
   */
  public final URI[] getURLArray(final String name, final URI[] def) {
    return get(name, def, URI[].class);
  }
}
