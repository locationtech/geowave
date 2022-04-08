/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.grpc.services;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParametersDelegate;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GeoWaveGrpcServiceCommandUtil {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcServiceCommandUtil.class.getName());

  static void setGrpcToCommandFields(
      final Map<FieldDescriptor, Object> m,
      final ServiceEnabledCommand cmd) {
    for (final Map.Entry<FieldDescriptor, Object> entry : m.entrySet()) {
      try {
        mapFieldValue(cmd, cmd.getClass(), entry);
      } catch (final IOException | IllegalArgumentException | IllegalAccessException e) {
        LOGGER.error("Exception encountered setting fields on command", e);
      }
    }
  }

  private static void mapFieldValue(
      final Object cmd,
      final Class<?> cmdClass,
      final Map.Entry<FieldDescriptor, Object> entry)
      throws IOException, IllegalArgumentException, IllegalAccessException {

    try {
      final Field currField = cmdClass.getDeclaredField(entry.getKey().getName());
      currField.setAccessible(true);
      Object value;
      if (entry.getValue() == null) {
        value = null;
      } else if (currField.getType().isArray() && (entry.getValue() instanceof List)) {
        // lets assume String as other arrays are not used with
        // JCommander at least currently
        // something like this line would have to be used to get the
        // class from the generic and instantiate an array of that class
        // GenericTypeResolver.resolveTypeArguments(entry.getValue().getClass(),
        // List.class)[0]
        value = ((List) entry.getValue()).toArray(new String[0]);
      } else {
        value = entry.getValue();
      }
      currField.set(cmd, value);
    } catch (final NoSuchFieldException e) {
      // scan the parameters delegates for the field if it could not be
      // found
      // as a stand-alone member
      final Field[] fields = cmdClass.getDeclaredFields();
      for (int i = 0; i < fields.length; i++) {
        if (fields[i].isAnnotationPresent(ParametersDelegate.class)) {
          fields[i].setAccessible(true);
          mapFieldValue((fields[i].get(cmd)), fields[i].getType(), entry);
        }
      }

      // bubble up through the class hierarchy
      if (cmdClass.getSuperclass() != null) {
        mapFieldValue(cmd, cmdClass.getSuperclass(), entry);
      }

    } catch (final IllegalAccessException e) {
      LOGGER.error("Exception encountered setting fields on command", e);
    }
  }
}
