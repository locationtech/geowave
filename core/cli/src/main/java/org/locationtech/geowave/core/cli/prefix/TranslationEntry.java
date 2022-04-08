/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.ResourceBundle;
import org.locationtech.geowave.core.cli.Constants;
import org.locationtech.geowave.core.cli.utils.JCommanderParameterUtils;
import com.beust.jcommander.Parameterized;

/**
 * This helper class is just a tuple that allows us to keep track of the parameters, their
 * translated field names, and the original object they map to.
 */
public class TranslationEntry {

  private final Parameterized param;
  private final Object object;
  private final String prefix;
  private final String[] prefixedNames;
  private final AnnotatedElement member;

  protected TranslationEntry(
      final Parameterized param,
      final Object object,
      final String prefix,
      final AnnotatedElement member) {
    this.param = param;
    this.object = object;
    this.prefix = prefix;
    this.member = member;
    prefixedNames = addPrefixToNames();
  }

  public Parameterized getParam() {
    return param;
  }

  public Object getObject() {
    return object;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isMethod() {
    return member instanceof Method;
  }

  public AnnotatedElement getMember() {
    return member;
  }

  public String[] getPrefixedNames() {
    return prefixedNames;
  }

  /**
   * Return the description for a field's parameter definition. If the parameter has a description
   * key specified, the description will be looked up in the resource bundle. If no description is
   * defined, the default CLI-specified description will be returned.
   *
   * @return the description
   */
  public String getDescription() {
    String description = null;
    // check to see if a description key is specified. If so, perform a
    // lookup in the GeoWave labels properties for a description to use
    // in place of the command line instance
    if ((getParam().getParameter() != null)
        && (getParam().getParameter().descriptionKey() != null)) {
      String descriptionKey = getParam().getParameter().descriptionKey();
      if ((descriptionKey != null) && !"".equals(descriptionKey.trim())) {
        descriptionKey = descriptionKey.trim();
        description = getDescriptionFromResourceBundle(descriptionKey);
      }
    } else if (getParam().isDynamicParameter()
        && (getParam().getWrappedParameter() != null)
        && (getParam().getWrappedParameter().getDynamicParameter() != null)) {
      String descriptionKey =
          getParam().getWrappedParameter().getDynamicParameter().descriptionKey();
      if ((descriptionKey != null) && !"".equals(descriptionKey.trim())) {
        descriptionKey = descriptionKey.trim();
        description = getDescriptionFromResourceBundle(descriptionKey);
      }
    }

    // if no description is set from GeoWave labels properties, use the one
    // set from the field parameter annotation definition
    if ((description == null) || "".equals(description.trim())) {
      if ((getParam().getParameter() != null)
          && (getParam().getParameter().description() != null)) {
        description = getParam().getParameter().description();
      } else if (getParam().isDynamicParameter()) {
        description = getParam().getWrappedParameter().getDynamicParameter().description();
      }
    }
    return description == null ? "<no description>" : description;
  }

  /**
   * If a parameter has a defined description key, this method will lookup the description for the
   * specified key.
   *
   * @param descriptionKey Key to lookup for description
   * @return the description
   */
  private String getDescriptionFromResourceBundle(final String descriptionKey) {
    String description = "";
    final String bundleName = Constants.GEOWAVE_DESCRIPTIONS_BUNDLE_NAME;
    final Locale locale = Locale.getDefault();
    final String defaultResourcePath = bundleName + ".properties";
    final String localeResourcePath = bundleName + "_" + locale.toString() + ".properties";
    if ((this.getClass().getResource("/" + defaultResourcePath) != null)
        || (this.getClass().getResource("/" + localeResourcePath) != null)) {

      // associate the default locale to the base properties, rather than
      // the standard resource bundle requiring a separate base
      // properties (GeoWaveLabels.properties) and a
      // default-locale-specific properties
      // (GeoWaveLabels_en_US.properties)
      final ResourceBundle resourceBundle =
          ResourceBundle.getBundle(
              bundleName,
              locale,
              ResourceBundle.Control.getNoFallbackControl(
                  ResourceBundle.Control.FORMAT_PROPERTIES));
      if (resourceBundle != null) {
        if (resourceBundle.containsKey(descriptionKey)) {
          description = resourceBundle.getString(descriptionKey);
        }
      }
    }
    return description;
  }

  /**
   * Specifies if this field is for a password.
   *
   * @return {@code true} if the field is a password
   */
  public boolean isPassword() {
    boolean isPassword = false;
    // check if a converter was specified. If so, if the converter is a
    // GeoWaveBaseConverter instance, check the isPassword value of the
    // converter
    isPassword = isPassword || JCommanderParameterUtils.isPassword(getParam().getParameter());
    isPassword =
        isPassword
            || JCommanderParameterUtils.isPassword(getParam().getWrappedParameter().getParameter());
    return isPassword;
  }

  /**
   * Specifies if this field is hidden.
   *
   * @return {@code true} if the field is hidden
   */
  public boolean isHidden() {
    if (getParam().getParameter() != null) {
      return getParam().getParameter().hidden();
    } else if (getParam().getWrappedParameter() != null) {
      return getParam().getWrappedParameter().hidden();
    }
    return false;
  }

  /**
   * Specifies if this field uses a string converter.
   *
   * @return {@code true} if the uses a string converter.
   */
  public boolean hasStringConverter() {
    if (getParam().getParameter() != null) {
      return getParam().getParameter().converter() != null;
    }
    return false;
  }

  /**
   * Specifies if this field is required.
   *
   * @return {@code true} if this field is required
   */
  public boolean isRequired() {
    boolean isRequired = false;
    isRequired = isRequired || JCommanderParameterUtils.isRequired(getParam().getParameter());
    isRequired =
        isRequired
            || JCommanderParameterUtils.isRequired(getParam().getWrappedParameter().getParameter());
    return isRequired;
  }

  /**
   * Whether the given object has a value specified. If the current value is non null, then return
   * true.
   *
   * @return {@code true} if this field has a value
   */
  public boolean hasValue() {
    final Object value = getParam().get(getObject());
    return value != null;
  }

  /**
   * Property name is used to write to properties files, but also to report option names to
   * Geoserver.
   *
   * @return the property name
   */
  public String getAsPropertyName() {
    return trimNonAlphabetic(getLongestParam(getPrefixedNames()));
  }

  /**
   * This function will take the configured prefix (a member variable) and add it to all the names
   * list.
   *
   * @return the list of new names
   */
  private String[] addPrefixToNames() {
    String[] names = null;
    if (param.getParameter() != null) {
      names = param.getParameter().names();
    } else {
      names = param.getWrappedParameter().names();
    }
    final String[] newNames = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      String item = names[i];
      final char subPrefix = item.charAt(0);
      int j = 0;
      while ((j < item.length()) && (item.charAt(j) == subPrefix)) {
        j++;
      }
      final String prePrefix = item.substring(0, j);
      item = item.substring(j);
      newNames[i] =
          String.format(
              "%s%s%s%s",
              prePrefix,
              prefix,
              JCommanderTranslationMap.PREFIX_SEPARATOR,
              item);
    }
    return newNames;
  }

  /**
   * For all the entries in names(), look for the largest one.
   *
   * @param names the names to check
   * @return the longest name
   */
  private String getLongestParam(final String[] names) {
    String longest = null;
    for (final String name : names) {
      if ((longest == null) || (name.length() > longest.length())) {
        longest = name;
      }
    }
    return longest;
  }

  /**
   * Remove any non alphabetic character from the beginning of the string. For example, '--version'
   * will become 'version'.
   *
   * @param str the string to trim
   * @return the trimmed string
   */
  private String trimNonAlphabetic(final String str) {
    int i = 0;
    for (i = 0; i < str.length(); i++) {
      if (Character.isAlphabetic(str.charAt(i))) {
        break;
      }
    }
    return str.substring(i);
  }
}
