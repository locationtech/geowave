/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;

/**
 * This class will translate a given set of ParameterDescription entries into a properties file or
 * back given a JCommander translation map.
 */
public class JCommanderPropertiesTransformer {

  private static Logger LOGGER = LoggerFactory.getLogger(JCommanderPropertiesTransformer.class);

  // The namespace is prepended to entries translated via
  // this translator in the Properties object, or it is used
  // to only retrieve properties that start with this
  // namespace.
  private final String propertyFormat;
  private final List<Object> objects = new ArrayList<>();

  public JCommanderPropertiesTransformer(final String namespace) {
    if (namespace == null) {
      propertyFormat = "%s";
    } else {
      propertyFormat = String.format("%s.%s", namespace, "%s");
    }
  }

  public JCommanderPropertiesTransformer() {
    this(null);
  }

  /**
   * Add an object to be translated
   *
   * @param object
   */
  public void addObject(final Object object) {
    objects.add(object);
  }

  /**
   * Entries are needed to translate to/from the objects using the JCommander prefixes.
   *
   * @return
   */
  private Collection<TranslationEntry> generateEntries() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    for (final Object obj : objects) {
      translator.addObject(obj);
    }
    final JCommanderTranslationMap map = translator.translate();
    return map.getEntries().values();
  }

  /**
   * Take the options and translate them to a map.
   *
   * @param properties
   */
  public void transformToMap(final Map<String, String> properties) {
    final Properties props = new Properties();
    transformToProperties(props);
    for (final String prop : props.stringPropertyNames()) {
      properties.put(prop, props.getProperty(prop));
    }
  }

  /**
   * Take the options and translate them from a map.
   *
   * @param properties
   */
  public void transformFromMap(final Map<String, String> properties) {
    final Properties props = new Properties();
    for (final Entry<String, String> prop : properties.entrySet()) {
      if (prop.getValue() != null) {
        props.setProperty(prop.getKey(), prop.getValue());
      }
    }
    transformFromProperties(props);
  }

  /**
   * Take the given values in the translation map, and convert them to a properties list.
   *
   * @param toProperties
   */
  public void transformToProperties(final Properties toProperties) {
    // Translate all fields.
    for (final TranslationEntry entry : generateEntries()) {

      // Get the Properties name.
      String propertyName = entry.getAsPropertyName();
      propertyName = String.format(propertyFormat, propertyName);

      // Get the value.
      Object value = null;
      try {
        value = entry.getParam().get(entry.getObject());
      } catch (final Exception e) {
        LOGGER.warn("Unable to set value", e);
        continue;
      }
      if (value == null) {
        continue;
      }
      // Dyn parameter, serialize map.
      if (entry.getParam().isDynamicParameter()) {
        @SuppressWarnings("unchecked")
        final Map<String, String> props = (Map<String, String>) value;
        for (final Map.Entry<String, String> prop : props.entrySet()) {
          if (prop.getValue() != null) {
            toProperties.put(String.format("%s.%s", propertyName, prop.getKey()), prop.getValue());
          }
        }
      } else {
        toProperties.put(propertyName, value.toString());
      }
    }
  }

  /**
   * Take the given properties list, and convert it to the given objects.
   *
   * @param fromProperties
   */
  public void transformFromProperties(final Properties fromProperties) {

    // This JCommander object is used strictly to use the 'convertValue'
    // function which happens to be public.
    final JCommander jc = new JCommander();

    // Translate all fields.
    for (final TranslationEntry entry : generateEntries()) {

      // Get the Properties name.
      String propertyName = entry.getAsPropertyName();
      propertyName = String.format(propertyFormat, propertyName);

      // Set the value.
      if (entry.getParam().isDynamicParameter()) {
        final Map<String, String> fromMap = new HashMap<>();
        final Set<String> propNames = fromProperties.stringPropertyNames();
        for (final String propName : propNames) {
          if (propName.startsWith(propertyName)) {
            // Parse
            final String parsedName = propName.substring(propertyName.length() + 1);
            fromMap.put(parsedName, fromProperties.getProperty(propName));
          }
        }
        // Set the map.
        entry.getParam().set(entry.getObject(), fromMap);
      } else {
        final String value = fromProperties.getProperty(propertyName);
        if (value != null) {
          // Convert the value to the expected format, and
          // set it on the original object.
          entry.getParam().set(
              entry.getObject(),
              jc.convertValue(entry.getParam(), entry.getParam().getType(), propertyName, value));
        }
      }
    }
  }
}
