/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.locationtech.geowave.core.cli.annotations.PrefixParameter;
import com.beust.jcommander.Parameterized;

/**
 * This class will take a collection of objects with JCommander annotations and create a transformed
 * set of objects with altered option prefixes, based on the @PrefixParameter annotation. It also
 * expands the capabilities of @ParametersDelegate, allowing you to specify a collection of objects,
 * or a map, where the String key is prepended as a prefix to the commands under that object. TODO:
 * This might work better with a Visitor pattern
 */
public class JCommanderPrefixTranslator {

  private final Queue<ParseContext> queue = new LinkedList<>();

  // These will be used to access the "field" or "method" attribute within
  // Parameterized,
  // which is a special JCommander class. If the interface changes in the
  // future, this
  // may not work anymore.
  private Field paraField;
  private Field paraMethod;

  public JCommanderPrefixTranslator() {
    try {
      // HP Fortify "Access Specifier Manipulation"
      // These fields are being modified by trusted code,
      // in a way that is not influenced by user input
      paraField = Parameterized.class.getDeclaredField("m_field");
      paraField.setAccessible(true);

      paraMethod = Parameterized.class.getDeclaredField("m_method");
      paraMethod.setAccessible(true);
    } catch (final NoSuchFieldException e) {
      // This is a programmer error, and will only happen if another
      // version of JCommander is being used.
      // newer versions of JCommander have renamed the member variables, try the new names
      try {
        paraField = Parameterized.class.getDeclaredField("field");

        paraField.setAccessible(true);

        paraMethod = Parameterized.class.getDeclaredField("method");
        paraMethod.setAccessible(true);
      } catch (NoSuchFieldException e2) {
        throw new RuntimeException(e);
      }
    }
  }

  public void addObject(final Object object) {
    final ParseContext pc = new ParseContext("", object);
    queue.add(pc);
  }

  public JCommanderTranslationMap translate() {

    // This map will hold the final translations
    final JCommanderTranslationMap transMap = new JCommanderTranslationMap();

    try {

      while (queue.size() > 0) {
        final ParseContext pc = queue.remove();
        final Object item = pc.getObject();

        // This is the JCommander class used to parse the object
        // hierarchy for
        // Parameter annotations. They kept it public ... so I used it.
        // Otherwise,
        // I'd have to parse all the annotations myself.
        final List<Parameterized> params = Parameterized.parseArg(item);

        // Iterate over the parameters, copying the method or field
        // parameters
        // into new parameters in 'newClass', ensuring that we maintain
        // annotations.
        for (final Parameterized param : params) {
          final Field f = (Field) paraField.get(param);
          final Method m = (Method) paraMethod.get(param);
          final AnnotatedElement annotatedElement = f != null ? f : m;

          // If this is a delegate, then process prefix parameter, add
          // the item
          // to the queue, and move on to the next field.
          if (param.getDelegateAnnotation() != null) {

            // JCommander only cares about non null fields when
            // processing
            // ParametersDelegate.
            final Object delegateItem = param.get(item);
            if (delegateItem != null) {

              // Prefix parameter only matters for
              // ParametersDelegate.
              final PrefixParameter prefixParam =
                  annotatedElement.getAnnotation(PrefixParameter.class);
              String newPrefix = pc.getPrefix();
              if (prefixParam != null) {
                if (!newPrefix.equals("")) {
                  newPrefix += JCommanderTranslationMap.PREFIX_SEPARATOR;
                }
                newPrefix += prefixParam.prefix();
              }

              // Is this a list type? If so then process each
              // object independently.
              if (delegateItem instanceof Collection) {
                final Collection<?> coll = (Collection<?>) delegateItem;
                for (final Object collItem : coll) {
                  final ParseContext newPc = new ParseContext(newPrefix, collItem);
                  queue.add(newPc);
                }
              }
              // For maps, use the key as an additional prefix
              // specifier.
              else if (delegateItem instanceof Map) {
                final Map<?, ?> mapp = (Map<?, ?>) delegateItem;
                for (final Map.Entry<?, ?> entry : mapp.entrySet()) {
                  final String prefix = entry.getKey().toString();
                  final Object mapItem = entry.getValue();
                  String convertedPrefix = newPrefix;
                  if (!convertedPrefix.equals("")) {
                    convertedPrefix += JCommanderTranslationMap.PREFIX_SEPARATOR;
                  }
                  convertedPrefix += prefix;
                  final ParseContext newPc = new ParseContext(convertedPrefix, mapItem);
                  queue.add(newPc);
                }
              }
              // Normal params delegate.
              else {
                final ParseContext newPc = new ParseContext(newPrefix, delegateItem);
                queue.add(newPc);
              }
            }
          } else {

            // TODO: In the future, if we wanted to do
            // @PluginParameter, this is probably
            // where we'd parse it, from annotatedElement. Then we'd
            // add it to
            // transMap below.

            // Rename the field so there are no conflicts. Name
            // really doesn't matter,
            // but it's used for translation in transMap.
            final String newFieldName = JavassistUtils.getNextUniqueFieldName();

            // Now add an entry to the translation map.
            transMap.addEntry(newFieldName, item, param, pc.getPrefix(), annotatedElement);
          }
        } // Iterate Parameterized
      } // Iterate Queue
      return transMap;
    } catch (final IllegalAccessException e) {
      // This should never happen, but if it does, then it's a programmer
      // error.
      throw new RuntimeException(e);
    }
  }

  /**
   * This class is used to keep context of what the current prefix is during prefix translation for
   * JCommander. It is stored in the queue.
   */
  private static class ParseContext {
    private final String prefix;
    private final Object object;

    public ParseContext(final String prefix, final Object object) {
      this.prefix = prefix;
      this.object = object;
    }

    public String getPrefix() {
      return prefix;
    }

    public Object getObject() {
      return object;
    }
  }
}
