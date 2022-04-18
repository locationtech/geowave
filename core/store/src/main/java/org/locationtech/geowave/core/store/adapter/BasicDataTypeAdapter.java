/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveFieldAnnotation;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

/**
 * A data type adapter implementation with explicit mappings for accessors and mutators. These
 * mappings can be automatically inferred from annotations or public properties via the static
 * `newAdapter` method.
 *
 * @param <T> the data type
 */
public class BasicDataTypeAdapter<T> extends AbstractDataTypeAdapter<T> {

  private Class<T> dataClass;
  private Constructor<T> objectConstructor;
  private Map<String, Accessor<T>> accessors;
  private Map<String, Mutator<T>> mutators;

  public BasicDataTypeAdapter() {}

  public BasicDataTypeAdapter(
      final String typeName,
      final Class<T> dataClass,
      final FieldDescriptor<?>[] fieldDescriptors,
      final FieldDescriptor<?> dataIDFieldDescriptor,
      final Map<String, Accessor<T>> accessors,
      final Map<String, Mutator<T>> mutators) {
    super(typeName, fieldDescriptors, dataIDFieldDescriptor);
    this.dataClass = dataClass;
    try {
      objectConstructor = dataClass.getDeclaredConstructor();
      objectConstructor.setAccessible(true);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(
          "A no-args constructor is required for object based data adapter classes.");
    }
    this.accessors = accessors;
    this.mutators = mutators;
  }

  @Override
  public Object getFieldValue(T entry, String fieldName) {
    if (accessors.containsKey(fieldName)) {
      return accessors.get(fieldName).get(entry);
    }
    return null;
  }

  @Override
  public T buildObject(final Object dataId, final Object[] fieldValues) {
    try {
      final T object = objectConstructor.newInstance();
      final FieldDescriptor<?>[] fields = getFieldDescriptors();
      for (int i = 0; i < fields.length; i++) {
        mutators.get(fields[i].fieldName()).set(object, fieldValues[i]);
      }
      if (!serializeDataIDAsString) {
        mutators.get(getDataIDFieldDescriptor().fieldName()).set(object, dataId);
      }
      return object;
    } catch (InstantiationException | IllegalAccessException | SecurityException
        | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException("Unable to build object.");
    }
  }

  @Override
  public byte[] toBinary() {
    final byte[] superBinary = super.toBinary();
    final byte[] classBytes = StringUtils.stringToBinary(dataClass.getName());
    int totalBytes =
        VarintUtils.unsignedIntByteLength(superBinary.length)
            + VarintUtils.unsignedIntByteLength(classBytes.length)
            + superBinary.length
            + classBytes.length;
    final FieldDescriptor<?>[] descriptors = getFieldDescriptors();
    for (final FieldDescriptor<?> descriptor : descriptors) {
      totalBytes += 1 + accessors.get(descriptor.fieldName()).byteCount();
      totalBytes += 1 + mutators.get(descriptor.fieldName()).byteCount();
    }
    totalBytes += 1 + accessors.get(getDataIDFieldDescriptor().fieldName()).byteCount();
    totalBytes += 1 + mutators.get(getDataIDFieldDescriptor().fieldName()).byteCount();
    final ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
    VarintUtils.writeUnsignedInt(superBinary.length, buffer);
    buffer.put(superBinary);
    VarintUtils.writeUnsignedInt(classBytes.length, buffer);
    buffer.put(classBytes);
    for (final FieldDescriptor<?> descriptor : descriptors) {
      final Accessor<T> accessor = accessors.get(descriptor.fieldName());
      final Mutator<T> mutator = mutators.get(descriptor.fieldName());
      buffer.put(accessor instanceof FieldAccessor ? (byte) 1 : (byte) 0);
      accessor.toBinary(buffer);
      buffer.put(mutator instanceof FieldMutator ? (byte) 1 : (byte) 0);
      mutator.toBinary(buffer);
    }
    final Accessor<T> accessor = accessors.get(getDataIDFieldDescriptor().fieldName());
    final Mutator<T> mutator = mutators.get(getDataIDFieldDescriptor().fieldName());
    buffer.put(accessor instanceof FieldAccessor ? (byte) 1 : (byte) 0);
    accessor.toBinary(buffer);
    buffer.put(mutator instanceof FieldMutator ? (byte) 1 : (byte) 0);
    mutator.toBinary(buffer);
    return buffer.array();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] superBinary = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(superBinary);
    super.fromBinary(superBinary);
    final byte[] classBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(classBytes);
    final String className = StringUtils.stringFromBinary(classBytes);
    try {
      dataClass = (Class) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to find data class for adapter: " + className);
    }
    try {
      objectConstructor = dataClass.getDeclaredConstructor();
      objectConstructor.setAccessible(true);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Unable to find no-args constructor for class: " + className);
    }
    final FieldDescriptor<?>[] descriptors = getFieldDescriptors();
    accessors = new HashMap<>(descriptors.length);
    mutators = new HashMap<>(descriptors.length);;
    for (final FieldDescriptor<?> descriptor : descriptors) {
      final Accessor<T> accessor;
      if (buffer.get() > 0) {
        accessor = new FieldAccessor<>();
      } else {
        accessor = new MethodAccessor<>();
      }
      accessor.fromBinary(dataClass, buffer);
      accessors.put(descriptor.fieldName(), accessor);
      final Mutator<T> mutator;
      if (buffer.get() > 0) {
        mutator = new FieldMutator<>();
      } else {
        mutator = new MethodMutator<>();
      }
      mutator.fromBinary(dataClass, buffer);
      mutators.put(descriptor.fieldName(), mutator);
    }
    if (buffer.hasRemaining()) {
      final Accessor<T> accessor;
      if (buffer.get() > 0) {
        accessor = new FieldAccessor<>();
      } else {
        accessor = new MethodAccessor<>();
      }
      accessor.fromBinary(dataClass, buffer);
      accessors.put(getDataIDFieldDescriptor().fieldName(), accessor);
      final Mutator<T> mutator;
      if (buffer.get() > 0) {
        mutator = new FieldMutator<>();
      } else {
        mutator = new MethodMutator<>();
      }
      mutator.fromBinary(dataClass, buffer);
      mutators.put(getDataIDFieldDescriptor().fieldName(), mutator);
    }
  }

  @Override
  public Class<T> getDataClass() {
    return dataClass;
  }

  /**
   * Create a new data type adapter from the specified class. If the class is annotated with
   * `@GeoWaveDataType`, all fields will be inferred from GeoWave field annotations. Otherwise
   * public fields and properties will be used. The data type field will also be encoded as a
   * regular field.
   * 
   * @param <T> the data type
   * @param typeName the type name for this adapter
   * @param dataClass the data type class
   * @param dataIdField the field to use for unique data IDs
   * @return the data adapter
   */
  public static <T> BasicDataTypeAdapter<T> newAdapter(
      final String typeName,
      final Class<T> dataClass,
      final String dataIdField) {
    return newAdapter(typeName, dataClass, dataIdField, false);
  }

  /**
   * Create a new data type adapter from the specified class. If the class is annotated with
   * `@GeoWaveDataType`, all fields will be inferred from GeoWave field annotations. Otherwise
   * public fields and properties will be used.
   * 
   * @param <T> the data type
   * @param typeName the type name for this adapter
   * @param dataClass the data type class
   * @param dataIdField the field to use for unique data IDs
   * @param removeDataIDFromFieldList if {@code true} the data ID field will not be included in the
   *        full list of fields, useful to prevent the data from being written twice at the cost of
   *        some querying simplicity
   * @return the data adapter
   */
  public static <T> BasicDataTypeAdapter<T> newAdapter(
      final String typeName,
      final Class<T> dataClass,
      final String dataIdField,
      final boolean removeDataIDFromFieldList) {
    final List<FieldDescriptor<?>> fieldDescriptors = new LinkedList<>();
    FieldDescriptor<?> dataIdFieldDescriptor = null;
    final Set<String> addedFields = new HashSet<>();
    final Map<String, Accessor<T>> accessors = new HashMap<>();
    final Map<String, Mutator<T>> mutators = new HashMap<>();
    if (dataClass.isAnnotationPresent(GeoWaveDataType.class)) {
      // infer fields from annotations
      Class<?> current = dataClass;
      while (!current.equals(Object.class)) {
        for (final Field f : current.getDeclaredFields()) {
          for (final Annotation a : f.getDeclaredAnnotations()) {
            if (a.annotationType().isAnnotationPresent(GeoWaveFieldAnnotation.class)) {
              try {
                final FieldDescriptor<?> descriptor =
                    a.annotationType().getAnnotation(
                        GeoWaveFieldAnnotation.class).fieldDescriptorBuilder().newInstance().buildFieldDescriptor(
                            f);
                checkWriterForClass(normalizeClass(f.getType()));
                if (addedFields.contains(descriptor.fieldName())) {
                  throw new RuntimeException("Duplicate field name: " + descriptor.fieldName());
                }
                f.setAccessible(true);
                accessors.put(descriptor.fieldName(), new FieldAccessor<>(f));
                mutators.put(descriptor.fieldName(), new FieldMutator<>(f));
                addedFields.add(descriptor.fieldName());
                if (descriptor.fieldName().equals(dataIdField)) {
                  dataIdFieldDescriptor = descriptor;
                  if (removeDataIDFromFieldList) {
                    continue;
                  }
                }
                fieldDescriptors.add(descriptor);
              } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(
                    "Unable to build field descriptor for field " + f.getName());
              }
            }
          }
        }
        current = current.getSuperclass();
      }
    } else {
      // Infer fields from properties and public fields
      try {
        final BeanInfo info = Introspector.getBeanInfo(dataClass);
        final PropertyDescriptor[] properties = info.getPropertyDescriptors();
        for (final PropertyDescriptor descriptor : properties) {
          if (descriptor.getName().equals("class")) {
            continue;
          }
          if (descriptor.getWriteMethod() == null) {
            continue;
          }
          if (descriptor.getReadMethod() == null) {
            continue;
          }
          final Class<?> type = normalizeClass(descriptor.getPropertyType());
          checkWriterForClass(type);
          accessors.put(descriptor.getName(), new MethodAccessor<>(descriptor.getReadMethod()));
          mutators.put(descriptor.getName(), new MethodMutator<>(descriptor.getWriteMethod()));
          addedFields.add(descriptor.getName());
          final FieldDescriptor<?> fieldDescriptor =
              new FieldDescriptorBuilder<>(type).fieldName(descriptor.getName()).build();
          if (fieldDescriptor.fieldName().equals(dataIdField)) {
            dataIdFieldDescriptor = fieldDescriptor;
            if (removeDataIDFromFieldList) {
              continue;
            }
          }
          fieldDescriptors.add(fieldDescriptor);
        }
      } catch (IntrospectionException e) {
        // Ignore
      }
      // Get public fields
      final Field[] fields = dataClass.getFields();
      for (final Field field : fields) {
        if (addedFields.contains(field.getName())) {
          continue;
        }
        final Class<?> type = normalizeClass(field.getType());
        checkWriterForClass(type);
        accessors.put(field.getName(), new FieldAccessor<>(field));
        mutators.put(field.getName(), new FieldMutator<>(field));
        final FieldDescriptor<?> fieldDescriptor =
            new FieldDescriptorBuilder<>(type).fieldName(field.getName()).build();
        if (fieldDescriptor.fieldName().equals(dataIdField)) {
          dataIdFieldDescriptor = fieldDescriptor;
          if (removeDataIDFromFieldList) {
            continue;
          }
        }
        fieldDescriptors.add(fieldDescriptor);
      }
    }
    return new BasicDataTypeAdapter<>(
        typeName,
        dataClass,
        fieldDescriptors.toArray(new FieldDescriptor<?>[fieldDescriptors.size()]),
        dataIdFieldDescriptor,
        accessors,
        mutators);
  }

  private static void checkWriterForClass(final Class<?> type) {
    final FieldWriter<?> writer = FieldUtils.getDefaultWriterForClass(type);
    if (writer == null) {
      throw new RuntimeException("No field reader/writer available for type: " + type.getName());
    }
  }

  private static interface Accessor<T> {
    Object get(T entry);

    int byteCount();

    void toBinary(ByteBuffer buffer);

    void fromBinary(final Class<T> dataClass, ByteBuffer buffer);
  }

  private static class MethodAccessor<T> implements Accessor<T> {
    private Method accessor;

    public MethodAccessor() {}

    public MethodAccessor(final Method accessorMethod) {
      this.accessor = accessorMethod;
    }

    @Override
    public Object get(final T entry) {
      try {
        return accessor.invoke(entry);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new RuntimeException("Unable to get value from entry", e);
      }
    }

    private byte[] nameBytes;

    @Override
    public int byteCount() {
      nameBytes = StringUtils.stringToBinary(accessor.getName());
      return nameBytes.length + VarintUtils.unsignedIntByteLength(nameBytes.length);
    }

    @Override
    public void toBinary(final ByteBuffer buffer) {
      VarintUtils.writeUnsignedInt(nameBytes.length, buffer);
      buffer.put(nameBytes);
    }

    @Override
    public void fromBinary(final Class<T> dataClass, final ByteBuffer buffer) {
      nameBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(nameBytes);
      final String name = StringUtils.stringFromBinary(nameBytes);
      try {
        accessor = dataClass.getMethod(name);
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Unable to find accessor method: " + name);
      }
    }
  }

  private static class FieldAccessor<T> implements Accessor<T> {
    private Field field;

    public FieldAccessor() {}

    public FieldAccessor(final Field field) {
      this.field = field;
    }

    @Override
    public Object get(final T entry) {
      try {
        return field.get(entry);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new RuntimeException("Unable to get value from entry", e);
      }
    }

    private byte[] nameBytes;

    @Override
    public int byteCount() {
      nameBytes = StringUtils.stringToBinary(field.getName());
      return nameBytes.length + VarintUtils.unsignedIntByteLength(nameBytes.length);
    }

    @Override
    public void toBinary(final ByteBuffer buffer) {
      VarintUtils.writeUnsignedInt(nameBytes.length, buffer);
      buffer.put(nameBytes);
    }

    @Override
    public void fromBinary(final Class<T> dataClass, final ByteBuffer buffer) {
      nameBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(nameBytes);
      final String name = StringUtils.stringFromBinary(nameBytes);
      field = findField(dataClass, name);
      if (field == null) {
        throw new RuntimeException("Unable to find field: " + name);
      }
      field.setAccessible(true);
    }
  }

  private static interface Mutator<T> {
    void set(T entry, Object value);

    int byteCount();

    void toBinary(ByteBuffer buffer);

    void fromBinary(final Class<T> dataClass, final ByteBuffer buffer);
  }

  private static class MethodMutator<T> implements Mutator<T> {
    private Method mutator;

    public MethodMutator() {}

    public MethodMutator(final Method mutator) {
      this.mutator = mutator;
    }

    @Override
    public void set(final T entry, final Object object) {
      try {
        mutator.invoke(entry, object);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new RuntimeException("Unable to set value on entry", e);
      }
    }

    private byte[] nameBytes;
    private byte[] parameterClassBytes;

    @Override
    public int byteCount() {
      nameBytes = StringUtils.stringToBinary(mutator.getName());
      parameterClassBytes = StringUtils.stringToBinary(mutator.getParameterTypes()[0].getName());
      return nameBytes.length
          + parameterClassBytes.length
          + VarintUtils.unsignedIntByteLength(nameBytes.length)
          + VarintUtils.unsignedIntByteLength(parameterClassBytes.length)
          + 1;
    }

    @Override
    public void toBinary(final ByteBuffer buffer) {
      VarintUtils.writeUnsignedInt(nameBytes.length, buffer);
      buffer.put(nameBytes);
      VarintUtils.writeUnsignedInt(parameterClassBytes.length, buffer);
      buffer.put(parameterClassBytes);
      if (mutator.getParameterTypes()[0].isPrimitive()) {
        buffer.put((byte) 1);
      } else {
        buffer.put((byte) 0);
      }
    }


    @Override
    public void fromBinary(final Class<T> dataClass, final ByteBuffer buffer) {
      nameBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(nameBytes);
      final String name = StringUtils.stringFromBinary(nameBytes);
      parameterClassBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(parameterClassBytes);
      final String parameterClassName = StringUtils.stringFromBinary(parameterClassBytes);
      final boolean isPrimitive = buffer.hasRemaining() && buffer.get() == (byte) 1;
      Class<?> parameterClass;
      try {
        if (isPrimitive) {
          parameterClass = getPrimitiveClass(parameterClassName);
        } else {
          parameterClass = Class.forName(parameterClassName);
        }
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(
            "Unable to find class for mutator parameter: " + parameterClassName);
      }
      try {
        mutator = dataClass.getMethod(name, parameterClass);
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Unable to find mutator method: " + name);
      }
    }
  }

  private static class FieldMutator<T> implements Mutator<T> {
    private Field field;

    public FieldMutator() {}

    public FieldMutator(final Field field) {
      this.field = field;
    }

    @Override
    public void set(final T entry, final Object object) {
      try {
        field.set(entry, object);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new RuntimeException("Unable to set value on entry", e);
      }
    }

    private byte[] nameBytes;

    @Override
    public int byteCount() {
      nameBytes = StringUtils.stringToBinary(field.getName());
      return nameBytes.length + VarintUtils.unsignedIntByteLength(nameBytes.length);
    }

    @Override
    public void toBinary(final ByteBuffer buffer) {
      VarintUtils.writeUnsignedInt(nameBytes.length, buffer);
      buffer.put(nameBytes);
    }

    @Override
    public void fromBinary(final Class<T> dataClass, final ByteBuffer buffer) {
      nameBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(nameBytes);
      final String name = StringUtils.stringFromBinary(nameBytes);
      field = findField(dataClass, name);
      if (field == null) {
        throw new RuntimeException("Unable to find field: " + name);
      }
      field.setAccessible(true);
    }
  }

  private static Field findField(final Class<?> dataClass, final String fieldName) {
    Class<?> current = dataClass;
    while (!current.equals(Object.class)) {
      try {
        final Field field = current.getDeclaredField(fieldName);
        return field;
      } catch (SecurityException | NoSuchFieldException e) {
        // Do nothing
      }
      current = current.getSuperclass();
    }
    return null;
  }

  public static Class<?> normalizeClass(final Class<?> sourceClass) {
    if (boolean.class.equals(sourceClass)) {
      return Boolean.class;
    }
    if (char.class.equals(sourceClass)) {
      return Character.class;
    }
    if (byte.class.equals(sourceClass)) {
      return Byte.class;
    }
    if (short.class.equals(sourceClass)) {
      return Short.class;
    }
    if (int.class.equals(sourceClass)) {
      return Integer.class;
    }
    if (long.class.equals(sourceClass)) {
      return Long.class;
    }
    if (float.class.equals(sourceClass)) {
      return Float.class;
    }
    if (double.class.equals(sourceClass)) {
      return Double.class;
    }
    return sourceClass;
  }

  public static Class<?> getPrimitiveClass(final String className) throws ClassNotFoundException {
    switch (className) {
      case "boolean":
        return boolean.class;
      case "char":
        return char.class;
      case "byte":
        return byte.class;
      case "short":
        return short.class;
      case "int":
        return int.class;
      case "long":
        return long.class;
      case "float":
        return float.class;
      case "double":
        return double.class;
      default:
        break;
    }
    throw new ClassNotFoundException("Unknown primitive class " + className);
  }

}
