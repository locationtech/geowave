/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import static org.junit.Assert.fail;
import java.lang.reflect.Method;
import org.junit.Assert;
import org.junit.Test;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.IntegerMemberValue;

public class JavassistUtilsTest {

  @Test
  public void testCloneAnnotationsAttribute() {
    final CtClass clz = ClassPool.getDefault().makeClass("testCloneAnnotationsAttribute");
    final CtMethod ctmethod = addNewMethod(clz, "origMethod");
    final AnnotationsAttribute attr = annotateMethod(ctmethod, "origAnno", 135);

    final AnnotationsAttribute clonedAttr =
        JavassistUtils.cloneAnnotationsAttribute(
            ctmethod.getMethodInfo().getConstPool(),
            attr,
            java.lang.annotation.ElementType.METHOD);

    Assert.assertEquals(
        135,
        ((IntegerMemberValue) clonedAttr.getAnnotation("java.lang.Integer").getMemberValue(
            "origAnno")).getValue());
  }

  private static class FindMethodTest {
    public void method1() {
      return;
    }

    public void methodA() {
      return;
    }
  }

  @Test
  public void testFindMethod() {
    final CtClass ctclass = ClassPool.getDefault().makeClass("testFindMethodClass");
    addNewMethod(ctclass, "method1");
    addNewMethod(ctclass, "method2");

    Method m = null;
    try {
      m = FindMethodTest.class.getMethod("method1");
    } catch (NoSuchMethodException | SecurityException e1) {
      e1.printStackTrace();
      return;
    }

    try {
      final CtMethod foundMethod = JavassistUtils.findMethod(ctclass, m);
      Assert.assertEquals("method1", foundMethod.getName());
    } catch (final NotFoundException e) {
      e.printStackTrace();
      fail("Could not find method in CtClass");
    }
  }

  @Test
  public void testCopyClassAnnontations() {
    final CtClass fromClass = ClassPool.getDefault().makeClass("fromClass");
    final CtClass toClass = ClassPool.getDefault().makeClass("toClass");

    // Create class annotations
    final ConstPool fromPool = fromClass.getClassFile().getConstPool();
    final AnnotationsAttribute attr =
        new AnnotationsAttribute(fromPool, AnnotationsAttribute.visibleTag);
    final Annotation anno = new Annotation("java.lang.Integer", fromPool);
    anno.addMemberValue("copyClassName", new IntegerMemberValue(fromPool, 246));
    attr.addAnnotation(anno);
    fromClass.getClassFile().addAttribute(attr);

    JavassistUtils.copyClassAnnotations(fromClass, toClass);

    final Annotation toAnno =
        ((AnnotationsAttribute) toClass.getClassFile().getAttribute(
            AnnotationsAttribute.visibleTag)).getAnnotation("java.lang.Integer");

    Assert.assertEquals(
        246,
        ((IntegerMemberValue) toAnno.getMemberValue("copyClassName")).getValue());
  }

  @Test
  public void testCopyMethodAnnotationsToField() {

    final CtClass ctclass = ClassPool.getDefault().makeClass("test");

    final CtMethod createdMethod = addNewMethod(ctclass, "doNothing");
    annotateMethod(createdMethod, "value", 123);

    final CtField createdField = addNewField(ctclass, "toField");

    JavassistUtils.copyMethodAnnotationsToField(createdMethod, createdField);

    IntegerMemberValue i = null;
    for (final Annotation annot : ((AnnotationsAttribute) createdField.getFieldInfo().getAttribute(
        AnnotationsAttribute.visibleTag)).getAnnotations()) {
      i = (IntegerMemberValue) annot.getMemberValue("value");
      if (i != null) {
        break;
      }
    }
    if ((i == null) || (i.getValue() != 123)) {
      fail("Expected annotation value 123 but found " + i);
    }
  }

  @Test
  public void testGetNextUniqueClassName() {
    final String unique1 = JavassistUtils.getNextUniqueClassName();
    final String unique2 = JavassistUtils.getNextUniqueClassName();

    Assert.assertFalse(unique1.equals(unique2));
  }

  @Test
  public void testGetNextUniqueFieldName() {
    final String unique1 = JavassistUtils.getNextUniqueFieldName();
    final String unique2 = JavassistUtils.getNextUniqueFieldName();

    Assert.assertFalse(unique1.equals(unique2));
  }

  @Test
  public void testGenerateEmptyClass() {
    final CtClass emptyClass = JavassistUtils.generateEmptyClass();
    final CtClass anotherEmptyClass = JavassistUtils.generateEmptyClass();

    Assert.assertFalse(emptyClass.equals(anotherEmptyClass));

    // test empty class works as expected
    final CtMethod method = addNewMethod(emptyClass, "a");
    annotateMethod(method, "abc", 7);
    final CtField field = addNewField(emptyClass, "d");
    annotateField(field, "def", 9);

    Assert.assertEquals(
        7,
        ((IntegerMemberValue) ((AnnotationsAttribute) method.getMethodInfo().getAttribute(
            AnnotationsAttribute.visibleTag)).getAnnotation("java.lang.Integer").getMemberValue(
                "abc")).getValue());

    Assert.assertEquals(
        9,
        ((IntegerMemberValue) ((AnnotationsAttribute) field.getFieldInfo().getAttribute(
            AnnotationsAttribute.visibleTag)).getAnnotation("java.lang.Integer").getMemberValue(
                "def")).getValue());
  }

  class TestClass {
    int field1;
    String field2;

    public void doNothing() {
      return;
    }
  }

  private CtMethod addNewMethod(final CtClass clz, final String methodName) {
    CtMethod ctmethod = null;
    try {
      ctmethod = CtNewMethod.make("void " + methodName + "(){ return; }", clz);
      clz.addMethod(ctmethod);
    } catch (final CannotCompileException e) {
      e.printStackTrace();
    }
    if (ctmethod == null) {
      fail("Could not create method");
    }

    return ctmethod;
  }

  private AnnotationsAttribute annotateMethod(
      final CtMethod ctmethod,
      final String annotationName,
      final int annotationValue) {
    final AnnotationsAttribute attr =
        new AnnotationsAttribute(
            ctmethod.getMethodInfo().getConstPool(),
            AnnotationsAttribute.visibleTag);
    final Annotation anno =
        new Annotation("java.lang.Integer", ctmethod.getMethodInfo().getConstPool());
    anno.addMemberValue(
        annotationName,
        new IntegerMemberValue(ctmethod.getMethodInfo().getConstPool(), annotationValue));
    attr.addAnnotation(anno);

    ctmethod.getMethodInfo().addAttribute(attr);

    return attr;
  }

  private CtField addNewField(final CtClass clz, final String fieldName) {
    CtField ctfield = null;
    try {
      ctfield = new CtField(clz, fieldName, clz);
      clz.addField(ctfield);
    } catch (final CannotCompileException e) {
      e.printStackTrace();
    }
    if (ctfield == null) {
      fail("Could not create method");
    }

    return ctfield;
  }

  private void annotateField(
      final CtField ctfield,
      final String annotationName,
      final int annotationValue) {
    final AnnotationsAttribute attr =
        new AnnotationsAttribute(
            ctfield.getFieldInfo().getConstPool(),
            AnnotationsAttribute.visibleTag);
    final Annotation anno =
        new Annotation("java.lang.Integer", ctfield.getFieldInfo().getConstPool());
    anno.addMemberValue(
        annotationName,
        new IntegerMemberValue(ctfield.getFieldInfo().getConstPool(), annotationValue));
    attr.addAnnotation(anno);

    ctfield.getFieldInfo().addAttribute(attr);
  }
}
