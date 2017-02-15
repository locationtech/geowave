package mil.nga.giat.geowave.core.cli.prefix;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.Test;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.AttributeInfo;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.IntegerMemberValue;
import junit.framework.Assert;
import mil.nga.giat.geowave.core.cli.prefix.JavassistUtils;

public class JavassistUtilsTest
{

	@Test
	public void testCloneAnnotationsAttribute() {
		CtClass clz = ClassPool.getDefault().makeClass(
				"testCloneAnnotationsAttribute");
		CtMethod ctmethod = addNewMethod(
				clz,
				"origMethod");
		AnnotationsAttribute attr = annotateMethod(
				ctmethod,
				"origAnno",
				135);

		AnnotationsAttribute clonedAttr = JavassistUtils.cloneAnnotationsAttribute(
				ctmethod.getMethodInfo().getConstPool(),
				attr,
				java.lang.annotation.ElementType.METHOD);

		Assert.assertEquals(
				135,
				((IntegerMemberValue) clonedAttr.getAnnotation(
						"java.lang.Integer").getMemberValue(
						"origAnno")).getValue());
	}

	private static class FindMethodTest
	{
		public void method1() {
			return;
		}

		public void methodA() {
			return;
		}
	}

	@Test
	public void testFindMethod() {
		CtClass ctclass = ClassPool.getDefault().makeClass(
				"testFindMethodClass");
		addNewMethod(
				ctclass,
				"method1");
		addNewMethod(
				ctclass,
				"method2");

		Method m = null;
		try {
			m = FindMethodTest.class.getMethod("method1");
		}
		catch (NoSuchMethodException | SecurityException e1) {
			e1.printStackTrace();
			return;
		}

		try {
			CtMethod foundMethod = JavassistUtils.findMethod(
					ctclass,
					m);
			Assert.assertEquals(
					"method1",
					foundMethod.getName());
		}
		catch (NotFoundException e) {
			e.printStackTrace();
			fail("Could not find method in CtClass");
		}
	}

	@Test
	public void testCopyClassAnnontations() {
		CtClass fromClass = ClassPool.getDefault().makeClass(
				"fromClass");
		CtClass toClass = ClassPool.getDefault().makeClass(
				"toClass");

		// Create class annotations
		ConstPool fromPool = fromClass.getClassFile().getConstPool();
		AnnotationsAttribute attr = new AnnotationsAttribute(
				fromPool,
				AnnotationsAttribute.visibleTag);
		Annotation anno = new Annotation(
				"java.lang.Integer",
				fromPool);
		anno.addMemberValue(
				"copyClassName",
				new IntegerMemberValue(
						fromPool,
						246));
		attr.addAnnotation(anno);
		fromClass.getClassFile().addAttribute(
				attr);

		JavassistUtils.copyClassAnnotations(
				fromClass,
				toClass);

		Annotation toAnno = ((AnnotationsAttribute) toClass.getClassFile().getAttribute(
				AnnotationsAttribute.visibleTag)).getAnnotation("java.lang.Integer");

		Assert.assertEquals(
				246,
				((IntegerMemberValue) toAnno.getMemberValue("copyClassName")).getValue());
	}

	@Test
	public void testCopyMethodAnnotationsToField() {

		CtClass ctclass = ClassPool.getDefault().makeClass(
				"test");

		CtMethod createdMethod = addNewMethod(
				ctclass,
				"doNothing");
		annotateMethod(
				createdMethod,
				"value",
				123);

		CtField createdField = addNewField(
				ctclass,
				"toField");

		JavassistUtils.copyMethodAnnotationsToField(
				createdMethod,
				createdField);

		IntegerMemberValue i = null;
		for (Annotation annot : ((AnnotationsAttribute) createdField.getFieldInfo().getAttribute(
				AnnotationsAttribute.visibleTag)).getAnnotations()) {
			i = (IntegerMemberValue) annot.getMemberValue("value");
			if (i != null) {
				break;
			}
		}
		if (i == null || i.getValue() != 123) {
			fail("Expected annotation value 123 but found " + i);
		}
	}

	@Test
	public void testGetNextUniqueClassName() {
		String unique1 = JavassistUtils.getNextUniqueClassName();
		String unique2 = JavassistUtils.getNextUniqueClassName();

		Assert.assertFalse(unique1.equals(unique2));
	}

	@Test
	public void testGetNextUniqueFieldName() {
		String unique1 = JavassistUtils.getNextUniqueFieldName();
		String unique2 = JavassistUtils.getNextUniqueFieldName();

		Assert.assertFalse(unique1.equals(unique2));
	}

	@Test
	public void testGenerateEmptyClass() {
		CtClass emptyClass = JavassistUtils.generateEmptyClass();
		CtClass anotherEmptyClass = JavassistUtils.generateEmptyClass();

		Assert.assertFalse(emptyClass.equals(anotherEmptyClass));

		// test empty class works as expected
		CtMethod method = addNewMethod(
				emptyClass,
				"a");
		annotateMethod(
				method,
				"abc",
				7);
		CtField field = addNewField(
				emptyClass,
				"d");
		annotateField(
				field,
				"def",
				9);

		Assert.assertEquals(
				7,
				((IntegerMemberValue) ((AnnotationsAttribute) method.getMethodInfo().getAttribute(
						AnnotationsAttribute.visibleTag)).getAnnotation(
						"java.lang.Integer").getMemberValue(
						"abc")).getValue());

		Assert.assertEquals(
				9,
				((IntegerMemberValue) ((AnnotationsAttribute) field.getFieldInfo().getAttribute(
						AnnotationsAttribute.visibleTag)).getAnnotation(
						"java.lang.Integer").getMemberValue(
						"def")).getValue());
	}

	class TestClass
	{
		int field1;
		String field2;

		public void doNothing() {
			return;
		}
	}

	private CtMethod addNewMethod(
			CtClass clz,
			String methodName ) {
		CtMethod ctmethod = null;
		try {
			ctmethod = CtNewMethod.make(
					"void " + methodName + "(){ return; }",
					clz);
			clz.addMethod(ctmethod);
		}
		catch (CannotCompileException e) {
			e.printStackTrace();
		}
		if (ctmethod == null) {
			fail("Could not create method");
		}

		return ctmethod;
	}

	private AnnotationsAttribute annotateMethod(
			CtMethod ctmethod,
			String annotationName,
			int annotationValue ) {
		AnnotationsAttribute attr = new AnnotationsAttribute(
				ctmethod.getMethodInfo().getConstPool(),
				AnnotationsAttribute.visibleTag);
		Annotation anno = new Annotation(
				"java.lang.Integer",
				ctmethod.getMethodInfo().getConstPool());
		anno.addMemberValue(
				annotationName,
				new IntegerMemberValue(
						ctmethod.getMethodInfo().getConstPool(),
						annotationValue));
		attr.addAnnotation(anno);

		ctmethod.getMethodInfo().addAttribute(
				attr);

		return attr;
	}

	private CtField addNewField(
			CtClass clz,
			String fieldName ) {
		CtField ctfield = null;
		try {
			ctfield = new CtField(
					clz,
					fieldName,
					clz);
			clz.addField(ctfield);
		}
		catch (CannotCompileException e) {
			e.printStackTrace();
		}
		if (ctfield == null) {
			fail("Could not create method");
		}

		return ctfield;
	}

	private void annotateField(
			CtField ctfield,
			String annotationName,
			int annotationValue ) {
		AnnotationsAttribute attr = new AnnotationsAttribute(
				ctfield.getFieldInfo().getConstPool(),
				AnnotationsAttribute.visibleTag);
		Annotation anno = new Annotation(
				"java.lang.Integer",
				ctfield.getFieldInfo().getConstPool());
		anno.addMemberValue(
				annotationName,
				new IntegerMemberValue(
						ctfield.getFieldInfo().getConstPool(),
						annotationValue));
		attr.addAnnotation(anno);

		ctfield.getFieldInfo().addAttribute(
				attr);
	}
}
