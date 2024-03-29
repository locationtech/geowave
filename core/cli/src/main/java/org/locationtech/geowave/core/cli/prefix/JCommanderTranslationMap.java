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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameterized;
import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.bytecode.AccessFlag;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.ArrayMemberValue;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.MemberValue;
import javassist.bytecode.annotation.StringMemberValue;

/**
 * The translation map allows us to easily copy values from the facade objects back to the original
 * objects.
 */
public class JCommanderTranslationMap {

  private static Logger LOGGER = LoggerFactory.getLogger(JCommanderTranslationMap.class);

  // This package is where classes generated by this translator live in the
  // classpath.
  public static final String NAMES_MEMBER = "names";
  public static final String REQUIRED_MEMBER = "required";
  // HP Fortify "Hardcoded Password - Password Management: Hardcoded Password"
  // false positive
  // This is a password label, not a password
  public static final String PASSWORD_MEMBER = "password";
  public static final String PREFIX_SEPARATOR = ".";

  // Tells us how to translate a field (indexed by facade field id) to
  // the original objects and back.
  private final Map<String, TranslationEntry> translations = new LinkedHashMap<>();

  // These are the objects generated by createFacadeObjects()
  private List<Object> translatedObjects = null;

  public JCommanderTranslationMap() {}

  /**
   * Objects are the facades.
   *
   * @return the translated objects
   */
  public Collection<Object> getObjects() {
    return Collections.unmodifiableCollection(translatedObjects);
  }

  /**
   * Return all the translations. They are indexed by 'field name', where field name is the field in
   * the facade object. Allow the user to modify them up until they create the facade objects
   *
   * @return the translations
   */
  public Map<String, TranslationEntry> getEntries() {
    if (translatedObjects != null) {
      return Collections.unmodifiableMap(translations);
    }
    return translations;
  }

  /**
   * Transfer the values from the facade objects to the original objects using the translation map.
   */
  public void transformToOriginal() {
    for (final Object obj : translatedObjects) {
      for (final Field field : obj.getClass().getDeclaredFields()) {
        final TranslationEntry tEntry = translations.get(field.getName());
        try {
          tEntry.getParam().set(tEntry.getObject(), field.get(obj));
        } catch (IllegalArgumentException | IllegalAccessException e) {
          // Allow these, since they really shouldn't ever happen.
          LOGGER.warn("Unable to return field object", e);
        }
      }
    }
  }

  /**
   * Transfer the values from the original objects to the facade objects using the translation map.
   */
  public void transformToFacade() {
    for (final Object obj : translatedObjects) {
      for (final Field field : obj.getClass().getDeclaredFields()) {
        final TranslationEntry tEntry = translations.get(field.getName());
        try {
          field.set(obj, tEntry.getParam().get(tEntry.getObject()));
        } catch (IllegalArgumentException | IllegalAccessException e) {
          // Ignore, no getter (if it's a method) or there was
          // a security violation.
          LOGGER.warn("Unable to set field", e);
        }
      }
    }
  }

  /**
   * This is a mapping between the created facade's field (e.g., field_0) and the JCommander
   * parameter (param) which lives in the object it was parsed from, 'item'.
   */
  protected void addEntry(
      final String newFieldName,
      final Object item,
      final Parameterized param,
      final String prefix,
      final AnnotatedElement member) {

    translations.put(newFieldName, new TranslationEntry(param, item, prefix, member));
  }

  /**
   * This will create the facade objects needed in order to parse the fields represented in the
   * translation map.
   */
  public void createFacadeObjects() {
    if (translatedObjects != null) {
      throw new RuntimeException("Cannot use the same translation " + "map twice");
    }

    // Clear old objects.
    translatedObjects = new ArrayList<>();

    // So we don't re-create classes we already created.
    final Map<Class<?>, CtClass> createdClasses = new HashMap<>();

    try {

      // This class pool will be used to find existing classes and create
      // new
      // classes.
      final ClassPool classPool = ClassPool.getDefault();
      final ClassClassPath path = new ClassClassPath(JCommanderPrefixTranslator.class);
      classPool.insertClassPath(path);

      // Iterate the final translations and create the classes.
      for (final Map.Entry<String, TranslationEntry> mapEntry : translations.entrySet()) {

        // Cache for later.
        final String newFieldName = mapEntry.getKey();
        final TranslationEntry entry = mapEntry.getValue();

        // This is the class we're making a facade of.
        final Class<?> objectClass = entry.getObject().getClass();

        // Get a CtClass reference to the item's class
        final CtClass oldClass = classPool.get(objectClass.getName());

        // Retrieve previously created class to add new field
        CtClass newClass = createdClasses.get(objectClass);

        // Create the class if we haven't yet.
        if (newClass == null) {

          // Create the class, so we can start adding the new facade
          // fields to it.
          newClass = JavassistUtils.generateEmptyClass();

          // Copy over the @Parameters annotation, if it is set.
          JavassistUtils.copyClassAnnotations(oldClass, newClass);

          // Store for later.
          createdClasses.put(objectClass, newClass);
        }

        // This is a field or method, which means we should add it to
        // our current
        // object.
        CtField newField = null;
        if (!entry.isMethod()) {
          // This is a field. This is easy! Just clone the field. It
          // will
          // copy over the annotations as well.
          newField = new CtField(oldClass.getField(entry.getParam().getName()), newClass);
        } else {
          // This is a method. This is hard. We can create a field
          // with the same name, but we gotta copy over the
          // annotations manually.
          // We also don't want to copy annotations that specifically
          // target
          // METHOD, so we'll only clone annotations that can target
          // FIELD.
          final CtClass fieldType = classPool.get(entry.getParam().getType().getName());
          newField = new CtField(fieldType, entry.getParam().getName(), newClass);

          // We need to find the existing method CtMethod reference,
          // so we can clone
          // annotations. This method is ugly. Do not look at it.
          final CtMethod method = JavassistUtils.findMethod(oldClass, (Method) entry.getMember());

          // Copy the annotations!
          JavassistUtils.copyMethodAnnotationsToField(method, newField);
        }

        // This is where the meat of the prefix algorithm is. If we have
        // a prefix
        // for this class(in ParseContext), then we apply it to the
        // attributes by
        // iterating over the annotations, looking for a 'names' member
        // variable, and
        // overriding the values one by one.
        if (entry.getPrefix().length() > 0) {
          overrideParameterPrefixes(newField, entry.getPrefixedNames());
        }

        // This is a fix for #95 (
        // https://github.com/cbeust/jcommander/issues/95 ).
        // I need this for cpstore, cpindex, etc, but it's only been
        // implemented as of 1.55,
        // an unreleased version.
        if (entry.isRequired() && entry.hasValue()) {
          disableBooleanMember(REQUIRED_MEMBER, newField);
        }

        if (entry.isPassword() && entry.hasValue()) {
          disableBooleanMember(PASSWORD_MEMBER, newField);
        }

        // Rename the field so there are no conflicts. Name really
        // doesn't matter,
        // but it's used for translation in transMap.
        newField.setName(newFieldName);
        newField.getFieldInfo().setAccessFlags(AccessFlag.PUBLIC);

        // Add the field to the class
        newClass.addField(newField);
      } // Iterate TranslationEntry

      // Convert the translated CtClass to an actual class.
      for (final CtClass clz : createdClasses.values()) {
        final Class<?> toClass = clz.toClass();
        final Object instance = toClass.newInstance();
        translatedObjects.add(instance);
      }
    } catch (InstantiationException | IllegalAccessException | NotFoundException
        | IllegalStateException | NullPointerException | CannotCompileException e) {
      LOGGER.error("Unable to create classes", e);
      throw new RuntimeException();
    }
    /*
     * catch (Exception e) { // This should never happen, but if it does, then it's a programmer //
     * error. throw new RuntimeException( e); }
     */
  }

  /**
   * Iterate the annotations, look for a 'names' parameter, and override it to prepend the given
   * prefix.
   */
  private void overrideParameterPrefixes(final CtField field, final String[] names) {

    // This is the JCommander package name
    final String packageName = JCommander.class.getPackage().getName();

    final AnnotationsAttribute fieldAttributes =
        (AnnotationsAttribute) field.getFieldInfo().getAttribute(AnnotationsAttribute.visibleTag);

    // Look for annotations that have a 'names' attribute, and whose package
    // starts with the expected JCommander package.
    for (final Annotation annotation : fieldAttributes.getAnnotations()) {
      if (annotation.getTypeName().startsWith(packageName)) {
        // See if it has a 'names' member variable.
        final MemberValue namesMember = annotation.getMemberValue(NAMES_MEMBER);

        // We have a names member!!!
        if (namesMember != null) {
          final ArrayMemberValue arrayNamesMember = (ArrayMemberValue) namesMember;

          // Iterate and transform each item in 'names()' list and
          // transform it.
          final MemberValue[] newMemberValues = new MemberValue[names.length];
          for (int i = 0; i < names.length; i++) {
            newMemberValues[i] =
                new StringMemberValue(names[i], field.getFieldInfo2().getConstPool());
          }

          // Override the member values in nameMember with the new
          // one's we've generated
          arrayNamesMember.setValue(newMemberValues);

          // This is KEY! For some reason, the existing annotation
          // will not be modified unless
          // you call 'setAnnotation' here. I'm guessing
          // 'getAnnotation()' creates a copy.
          fieldAttributes.setAnnotation(annotation);

          // Finished processing names.
          break;
        }
      }
    }
  }

  /**
   * Iterate the annotations, look for a 'required' parameter, and set it to false.
   */
  private void disableBooleanMember(final String booleanMemberName, final CtField field) {

    // This is the JCommander package name
    final String packageName = JCommander.class.getPackage().getName();

    final AnnotationsAttribute fieldAttributes =
        (AnnotationsAttribute) field.getFieldInfo().getAttribute(AnnotationsAttribute.visibleTag);

    // Look for annotations that have a 'names' attribute, and whose package
    // starts with the expected JCommander package.
    for (final Annotation annotation : fieldAttributes.getAnnotations()) {
      if (annotation.getTypeName().startsWith(packageName)) {
        // See if it has a 'names' member variable.
        final MemberValue requiredMember = annotation.getMemberValue(booleanMemberName);

        // We have a names member!!!
        if (requiredMember != null) {
          final BooleanMemberValue booleanRequiredMember = (BooleanMemberValue) requiredMember;

          // Set it to not required.
          booleanRequiredMember.setValue(false);

          // This is KEY! For some reason, the existing annotation
          // will not be modified unless
          // you call 'setAnnotation' here. I'm guessing
          // 'getAnnotation()' creates a copy.
          fieldAttributes.setAnnotation(annotation);

          // Finished processing names.
          break;
        }
      }
    }
  }
}
