/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.service.rest.GeoWaveOperationServiceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class RestFieldFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
  private static final BitSet UNESCAPED_CHARS = initUnescapedChars();

  @FunctionalInterface
  private interface ParameterInitializer<T extends RestField<?>> {
    public T apply(Field field, Parameter parameter, Object instance);
  }

  @FunctionalInterface
  private interface MainParamInitializer<T extends RestField<?>> {
    public T apply(
        String name,
        boolean isList,
        Field mainParamField,
        int subfieldOrdinal,
        int totalSize,
        Object instance);
  }

  public static List<RestField<?>> createRestFields(final Class<?> instanceType) {
    return internalCreateRestFields(
        // for just getting the fields we don't need to waste time on
        // using reflection to get an instance, that is only necessary
        // for setting values
        null,
        instanceType,
        (ParameterInitializer<RestField<?>>) (
            final Field field,
            final Parameter parameter,
            final Object instance) -> new ParameterRestField(field, parameter),
        (
            final String name,
            final boolean isList,
            final Field mainParamField,
            final int subfieldOrdinal,
            final int totalSize,
            final Object instance) -> new BasicRestField(
                name,
                isList ? List.class : String.class,
                "main parameter",
                true));
  }

  public static List<RestFieldValue<?>> createRestFieldValues(final Object instance) {
    return internalCreateRestFields(
        instance,
        instance.getClass(),
        (ParameterInitializer<RestFieldValue<?>>) (
            final Field field,
            final Parameter parameter,
            final Object i) -> new ParameterRestFieldValue(field, parameter, i),
        (
            final String name,
            final boolean isList,
            final Field mainParamField,
            final int subfieldOrdinal,
            final int totalSize,
            final Object i) -> isList
                ? new ListMainParam(
                    subfieldOrdinal,
                    totalSize,
                    mainParamField,
                    new BasicRestField<>(name, List.class, "main parameter", true),
                    instance)
                : new StringMainParam(
                    subfieldOrdinal,
                    totalSize,
                    mainParamField,
                    new BasicRestField<>(name, String.class, "main parameter", true),
                    instance));
  }

  private static <T extends RestField<?>> List<T> internalCreateRestFields(
      final Object instance,
      final Class<?> instanceType,
      final ParameterInitializer<T> parameterInitializer,
      final MainParamInitializer<T> mainParamInitializer) {
    final List<T> retVal = new ArrayList<>();
    for (final Field field : FieldUtils.getFieldsWithAnnotation(instanceType, Parameter.class)) {
      retVal.addAll(
          internalCreateRestFields(
              field,
              field.getAnnotation(Parameter.class),
              instance,
              parameterInitializer,
              mainParamInitializer));
    }

    for (final Field field : FieldUtils.getFieldsWithAnnotation(
        instanceType,
        ParametersDelegate.class)) {
      try {
        final Class<?> delegateInstanceType;
        Object delegateInstance;
        if (instance != null) {
          // HP Fortify "Access Control" false positive
          // The need to change the accessibility here is
          // necessary, has been review and judged to be safe
          field.setAccessible(true);

          delegateInstance = field.get(instance);
          if (delegateInstance == null) {
            delegateInstanceType = field.getType();
            delegateInstance = delegateInstanceType.newInstance();
          } else {
            delegateInstanceType = delegateInstance.getClass();
            if (delegateInstance instanceof Map) {
              for (final Object mapValueInstance : ((Map) delegateInstance).values()) {
                final Class<?> mapValueInstanceType = mapValueInstance.getClass();
                retVal.addAll(
                    internalCreateRestFields(
                        mapValueInstance,
                        mapValueInstanceType,
                        parameterInitializer,
                        mainParamInitializer));
              }
            }
          }
          field.set(instance, delegateInstance);
        } else {
          delegateInstanceType = field.getType();
          // here just assume if instance was null we don't need to
          // waste
          // time on reflection to make delegate instance
          delegateInstance = null;
        }
        retVal.addAll(
            internalCreateRestFields(
                delegateInstance,
                delegateInstanceType,
                parameterInitializer,
                mainParamInitializer));

      } catch (InstantiationException | IllegalAccessException e) {
        LOGGER.error("Unable to instantiate field", e);
      }
    }
    return retVal;
  }

  private static <T extends RestField<?>> List<T> internalCreateRestFields(
      final Field field,
      final Parameter parameter,
      final Object instance,
      final ParameterInitializer<T> parameterInitializer,
      final MainParamInitializer<T> mainParamInitializer) {
    // handle case for core/main params for a command
    // for now we parse based on assumptions within description
    // TODO see Issue #1185 for details on a more explicit main
    // parameter suggestion
    final String desc = parameter.description();
    // this is intended to match one or more "<" + at least one alphanumeric
    // or some select special character + ">"
    if (List.class.isAssignableFrom(field.getType())
        && !desc.isEmpty()
        && desc.matches("(<[a-zA-Z0-9:/\\s]+>\\s*)+")) {
      int currentEndParamIndex = 0;
      // this simply is collecting names and a flag to indicate if its a
      // list
      final List<Pair<String, Boolean>> individualParams = new ArrayList<>();
      do {
        final int currentStartParamIndex = desc.indexOf('<', currentEndParamIndex);
        if ((currentStartParamIndex < 0) || (currentStartParamIndex >= (desc.length() - 1))) {
          break;
        }
        currentEndParamIndex = desc.indexOf('>', currentStartParamIndex + 1);
        final String fullName =
            desc.substring(currentStartParamIndex + 1, currentEndParamIndex).trim();
        if (!fullName.isEmpty()) {
          if (fullName.startsWith("comma separated list of ")) {
            individualParams.add(ImmutablePair.of(fullName.substring(24).trim(), true));
          } else if (fullName.startsWith("comma delimited ")) {
            individualParams.add(ImmutablePair.of(fullName.substring(16).trim(), true));
          } else {
            individualParams.add(ImmutablePair.of(fullName, false));
          }
        }
      } while ((currentEndParamIndex > 0) && (currentEndParamIndex < desc.length()));
      final int totalSize = individualParams.size();
      return Lists.transform(individualParams, new Function<Pair<String, Boolean>, T>() {
        int i = 0;

        @Override
        public T apply(final Pair<String, Boolean> input) {
          if (input != null) {
            return mainParamInitializer.apply(
                toURLFriendlyString(input.getLeft()),
                input.getRight(),
                field,
                i++,
                totalSize,
                instance);
          } else {
            return null;
          }
        }
      });
    } else {
      return Collections.singletonList(parameterInitializer.apply(field, parameter, instance));
    }
  }

  public static String toURLFriendlyString(final String str) {
    boolean needToChange = false;
    final StringBuffer out = new StringBuffer(str.length());
    boolean capsNext = false;
    for (int i = 0; i < str.length(); i++) {
      final int c = str.charAt(i);
      if (UNESCAPED_CHARS.get(c)) {
        if (capsNext) {
          out.append(Character.toUpperCase((char) c));
          capsNext = false;
        } else {
          out.append((char) c);
        }
      } else {
        needToChange = true;
        capsNext = true;
      }
    }
    return (needToChange ? out.toString() : str);
  }

  private static BitSet initUnescapedChars() {
    final BitSet unescapedChars = new BitSet(256);
    int i;
    for (i = 'a'; i <= 'z'; i++) {
      unescapedChars.set(i);
    }
    for (i = 'A'; i <= 'Z'; i++) {
      unescapedChars.set(i);
    }
    for (i = '0'; i <= '9'; i++) {
      unescapedChars.set(i);
      unescapedChars.set('-');
      unescapedChars.set('_');
      unescapedChars.set('.');
      unescapedChars.set('*');
    }
    return unescapedChars;
  }
}
