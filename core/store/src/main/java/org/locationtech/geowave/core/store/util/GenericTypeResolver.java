/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.locationtech.geowave.core.store.util;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a derivative from hte Spring Framework library. Helper class for resolving generic
 * types against type variables.
 *
 * <p> Mainly intended for usage within the framework, resolving method parameter types even when
 * they are declared generically.
 *
 * @author Juergen Hoeller
 * @author Rob Harrop
 * @author Roy Clarkson
 * @since 1.0
 */
public abstract class GenericTypeResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTypeResolver.class);

  /** Cache from Class to TypeVariable Map */
  private static final Map<Class<?>, Reference<Map<TypeVariable<?>, Type>>> typeVariableCache =
      Collections.synchronizedMap(
          new WeakHashMap<Class<?>, Reference<Map<TypeVariable<?>, Type>>>());

  /**
   * Resolve the single type argument of the given generic interface against the given target class
   * which is assumed to implement the generic interface and possibly declare a concrete type for
   * its type variable.
   *
   * @param clazz the target class to check against
   * @param genericIfc the generic interface or superclass to resolve the type argument from
   * @return the resolved type of the argument, or <code>null</code> if not resolvable
   */
  public static Class<?> resolveTypeArgument(final Class<?> clazz, final Class<?> genericIfc) {
    final Class<?>[] typeArgs = resolveTypeArguments(clazz, genericIfc);
    if (typeArgs == null) {
      return null;
    }
    if (typeArgs.length != 1) {
      throw new IllegalArgumentException(
          "Expected 1 type argument on generic interface ["
              + genericIfc.getName()
              + "] but found "
              + typeArgs.length);
    }
    return typeArgs[0];
  }

  /**
   * Resolve the type arguments of the given generic interface against the given target class which
   * is assumed to implement the generic interface and possibly declare concrete types for its type
   * variables.
   *
   * @param clazz the target class to check against
   * @param genericIfc the generic interface or superclass to resolve the type argument from
   * @return the resolved type of each argument, with the array size matching the number of actual
   *         type arguments, or <code>null</code> if not resolvable
   */
  public static Class<?>[] resolveTypeArguments(final Class<?> clazz, final Class<?> genericIfc) {
    return doResolveTypeArguments(clazz, clazz, genericIfc);
  }

  private static Class<?>[] doResolveTypeArguments(
      final Class<?> ownerClass,
      Class<?> classToIntrospect,
      final Class<?> genericIfc) {
    while (classToIntrospect != null) {
      if (genericIfc.isInterface()) {
        final Type[] ifcs = classToIntrospect.getGenericInterfaces();
        for (final Type ifc : ifcs) {
          final Class<?>[] result = doResolveTypeArguments(ownerClass, ifc, genericIfc);
          if (result != null) {
            return result;
          }
        }
      } else {
        final Class<?>[] result =
            doResolveTypeArguments(
                ownerClass,
                classToIntrospect.getGenericSuperclass(),
                genericIfc);
        if (result != null) {
          return result;
        }
      }
      classToIntrospect = classToIntrospect.getSuperclass();
    }
    return null;
  }

  private static Class<?>[] doResolveTypeArguments(
      final Class<?> ownerClass,
      final Type ifc,
      final Class<?> genericIfc) {
    if (ifc instanceof ParameterizedType) {
      final ParameterizedType paramIfc = (ParameterizedType) ifc;
      final Type rawType = paramIfc.getRawType();
      if (genericIfc.equals(rawType)) {
        final Type[] typeArgs = paramIfc.getActualTypeArguments();
        final Class<?>[] result = new Class[typeArgs.length];
        for (int i = 0; i < typeArgs.length; i++) {
          final Type arg = typeArgs[i];
          result[i] = extractClass(ownerClass, arg);
        }
        return result;
      } else if (genericIfc.isAssignableFrom((Class<?>) rawType)) {
        return doResolveTypeArguments(ownerClass, (Class<?>) rawType, genericIfc);
      }
    } else if ((ifc != null) && genericIfc.isAssignableFrom((Class<?>) ifc)) {
      return doResolveTypeArguments(ownerClass, (Class<?>) ifc, genericIfc);
    }
    return null;
  }

  /** Extract a class instance from given Type. */
  private static Class<?> extractClass(final Class<?> ownerClass, Type arg) {
    if (arg instanceof ParameterizedType) {
      return extractClass(ownerClass, ((ParameterizedType) arg).getRawType());
    } else if (arg instanceof GenericArrayType) {
      final GenericArrayType gat = (GenericArrayType) arg;
      final Type gt = gat.getGenericComponentType();
      final Class<?> componentClass = extractClass(ownerClass, gt);
      return Array.newInstance(componentClass, 0).getClass();
    } else if (arg instanceof TypeVariable) {
      final TypeVariable<?> tv = (TypeVariable<?>) arg;
      arg = getTypeVariableMap(ownerClass).get(tv);
      if (arg == null) {
        arg = extractBoundForTypeVariable(tv);
      } else {
        arg = extractClass(ownerClass, arg);
      }
    }
    return (arg instanceof Class ? (Class<?>) arg : Object.class);
  }

  /**
   * Resolve the specified generic type against the given TypeVariable map.
   *
   * @param genericType the generic type to resolve
   * @param typeVariableMap the TypeVariable Map to resolved against
   * @return the type if it resolves to a Class, or <code>Object.class</code> otherwise
   */
  public static Class<?> resolveType(
      final Type genericType,
      final Map<TypeVariable<?>, Type> typeVariableMap) {
    final Type rawType = getRawType(genericType, typeVariableMap);
    return (rawType instanceof Class ? (Class<?>) rawType : Object.class);
  }

  /**
   * Determine the raw type for the given generic parameter type.
   *
   * @param genericType the generic type to resolve
   * @param typeVariableMap the TypeVariable Map to resolved against
   * @return the resolved raw type
   */
  static Type getRawType(final Type genericType, final Map<TypeVariable<?>, Type> typeVariableMap) {
    Type resolvedType = genericType;
    if (genericType instanceof TypeVariable) {
      final TypeVariable<?> tv = (TypeVariable<?>) genericType;
      resolvedType = typeVariableMap.get(tv);
      if (resolvedType == null) {
        resolvedType = extractBoundForTypeVariable(tv);
      }
    }
    if (resolvedType instanceof ParameterizedType) {
      return ((ParameterizedType) resolvedType).getRawType();
    } else {
      return resolvedType;
    }
  }

  /**
   * Build a mapping of {@link TypeVariable#getName TypeVariable names} to concrete {@link Class}
   * for the specified {@link Class}. Searches all super types, enclosing types and interfaces.
   */
  public static Map<TypeVariable<?>, Type> getTypeVariableMap(final Class<?> clazz) {
    final Reference<Map<TypeVariable<?>, Type>> ref = typeVariableCache.get(clazz);
    Map<TypeVariable<?>, Type> typeVariableMap = (ref != null ? ref.get() : null);

    if (clazz == null) {
      throw new IllegalArgumentException("clazz can not be null");
    }

    if (typeVariableMap == null) {
      typeVariableMap = new HashMap<>();

      // interfaces
      extractTypeVariablesFromGenericInterfaces(clazz.getGenericInterfaces(), typeVariableMap);

      // super class
      Type genericType = clazz.getGenericSuperclass();
      Class<?> type = clazz.getSuperclass();
      while ((type != null) && !Object.class.equals(type)) {
        if (genericType instanceof ParameterizedType) {
          final ParameterizedType pt = (ParameterizedType) genericType;
          populateTypeMapFromParameterizedType(pt, typeVariableMap);
        }
        extractTypeVariablesFromGenericInterfaces(type.getGenericInterfaces(), typeVariableMap);
        genericType = type.getGenericSuperclass();
        type = type.getSuperclass();
      }

      // enclosing class
      type = clazz;
      while (type.isMemberClass()) {
        genericType = type.getGenericSuperclass();
        if (genericType instanceof ParameterizedType) {
          final ParameterizedType pt = (ParameterizedType) genericType;
          populateTypeMapFromParameterizedType(pt, typeVariableMap);
        }
        type = type.getEnclosingClass();
        if (type == null) {
          LOGGER.error("type.getEnclosingClass() returned null");
          return null;
        }
      }

      typeVariableCache.put(clazz, new WeakReference<>(typeVariableMap));
    }

    return typeVariableMap;
  }

  /** Extracts the bound <code>Type</code> for a given {@link TypeVariable}. */
  static Type extractBoundForTypeVariable(final TypeVariable<?> typeVariable) {
    final Type[] bounds = typeVariable.getBounds();
    if (bounds.length == 0) {
      return Object.class;
    }
    Type bound = bounds[0];
    if (bound instanceof TypeVariable) {
      bound = extractBoundForTypeVariable((TypeVariable<?>) bound);
    }
    return bound;
  }

  private static void extractTypeVariablesFromGenericInterfaces(
      final Type[] genericInterfaces,
      final Map<TypeVariable<?>, Type> typeVariableMap) {
    for (final Type genericInterface : genericInterfaces) {
      if (genericInterface instanceof ParameterizedType) {
        final ParameterizedType pt = (ParameterizedType) genericInterface;
        populateTypeMapFromParameterizedType(pt, typeVariableMap);
        if (pt.getRawType() instanceof Class) {
          extractTypeVariablesFromGenericInterfaces(
              ((Class<?>) pt.getRawType()).getGenericInterfaces(),
              typeVariableMap);
        }
      } else if (genericInterface instanceof Class) {
        extractTypeVariablesFromGenericInterfaces(
            ((Class<?>) genericInterface).getGenericInterfaces(),
            typeVariableMap);
      }
    }
  }

  /**
   * Read the {@link TypeVariable TypeVariables} from the supplied {@link ParameterizedType} and add
   * mappings corresponding to the {@link TypeVariable#getName TypeVariable name} -> concrete type
   * to the supplied {@link Map}.
   *
   * <p> Consider this case:
   *
   * <pre class="code> public interface Foo<S, T> { .. }
   *
   * public class FooImpl implements Foo<String, Integer> { .. }
   * </pre>
   *
   * For '<code>FooImpl</code>' the following mappings would be added to the {@link Map}:
   * {S=java.lang.String, T=java.lang.Integer}.
   */
  private static void populateTypeMapFromParameterizedType(
      final ParameterizedType type,
      final Map<TypeVariable<?>, Type> typeVariableMap) {
    if (type.getRawType() instanceof Class) {
      final Type[] actualTypeArguments = type.getActualTypeArguments();
      final TypeVariable<?>[] typeVariables = ((Class<?>) type.getRawType()).getTypeParameters();
      for (int i = 0; i < actualTypeArguments.length; i++) {
        final Type actualTypeArgument = actualTypeArguments[i];
        final TypeVariable<?> variable = typeVariables[i];
        if (actualTypeArgument instanceof Class) {
          typeVariableMap.put(variable, actualTypeArgument);
        } else if (actualTypeArgument instanceof GenericArrayType) {
          typeVariableMap.put(variable, actualTypeArgument);
        } else if (actualTypeArgument instanceof ParameterizedType) {
          typeVariableMap.put(variable, actualTypeArgument);
        } else if (actualTypeArgument instanceof TypeVariable) {
          // We have a type that is parameterized at instantiation
          // time
          // the nearest match on the bridge method will be the
          // bounded type.
          final TypeVariable<?> typeVariableArgument = (TypeVariable<?>) actualTypeArgument;
          Type resolvedType = typeVariableMap.get(typeVariableArgument);
          if (resolvedType == null) {
            resolvedType = extractBoundForTypeVariable(typeVariableArgument);
          }
          typeVariableMap.put(variable, resolvedType);
        }
      }
    }
  }
}
