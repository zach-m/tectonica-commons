package com.tectonica.util;

import java.lang.reflect.Field;

/**
 * Simple reflection-based wrapper for a specific field in a specific instance. The field is identified by a dot-notation (e.g.
 * "address.city.name") and can be manipulated conveniently via the various methods.
 * 
 * @author Zach Melamed
 */
public class FieldAccessor
{
	private FieldAccessor()
	{}

	private Object instance;
	private String fieldPath;
	private Field field;

	public Object getInstance()
	{
		return instance;
	}

	public String getPath()
	{
		return fieldPath;
	}

	public Class<?> getType()
	{
		if (field == null)
			return null;
		return field.getType();
	}

	public static FieldAccessor of(Object instance, String fieldPath, boolean ensurePath)
	{
//		System.out.println("Looking for '" + fieldPath + "' in " + object.getClass().getName());
		FieldAccessor fa = new FieldAccessor();
		fa.fieldPath = fieldPath;
		fa.setFieldAndObject(instance, fieldPath, ensurePath);
		return fa;
	}

	private void setFieldAndObject(Object current, String fieldPath, boolean ensurePath)
	{
		int i = fieldPath.indexOf('.');
		if (i < 0)
		{
			// terminal field in the path, we initialize the internal values
			field = getField(current, fieldPath);
			field.setAccessible(true);
			instance = current;
			return;
		}

		// further drill-down is needed
		String fieldName = fieldPath.substring(0, i);
		String remainingPath = fieldPath.substring(i + 1);
		Object memberInstance = getMemberInstance(current, fieldName);
		if (memberInstance == null)
		{
			if (!ensurePath)
				return; // leaves the internal values uninitialized, setters won't work
			memberInstance = createMemberInstance(current, fieldName);
		}
		setFieldAndObject(memberInstance, remainingPath, ensurePath);
	}

	private Field getField(Object instance, String fieldName)
	{
		try
		{
			Class<?> clz = instance.getClass();
			while (!Object.class.equals(clz))
			{
				Field field = clz.getDeclaredField(fieldName);
				if (field != null)
					return field;
				clz = clz.getSuperclass();
			}
			return null;
		}
		catch (Exception e)
		{
			throw new RuntimeException("couldn't extract '" + fieldName + "' from " + instance, e);
		}
	}

	private Object getMemberInstance(Object instance, String fieldName)
	{
		Field field = getField(instance, fieldName);
		try
		{
			field.setAccessible(true);
			return field.get(instance);
		}
		catch (Exception e)
		{
			throw new RuntimeException("couldn't extract value of '" + fieldName + "' from " + instance, e);
		}
	}

	private Object createMemberInstance(Object instance, String fieldName)
	{
		try
		{
			Field memberField = getField(instance, fieldName);
			memberField.setAccessible(true);
			Object memberInstance = memberField.getType().newInstance();
			memberField.set(instance, memberInstance);
			return memberInstance;
		}
		catch (Exception e)
		{
			throw new RuntimeException("couldn't create member '" + fieldName + "' in " + instance, e);
		}
	}

	public Object getValue()
	{
		if (field == null)
			return null;

		try
		{
			return field.get(instance);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public void setValue(Object value)
	{
		if (field == null)
			throw new RuntimeException("Object containing " + fieldPath + " doesn't exist, use ensurePath=true");

		try
		{
			field.set(instance, value);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Similar to {@link #setValue(Object)}, but doesn't require that the passed object is of the same class as the existing field. It will
	 * perform conversions where feasible. Specifically:
	 * <ul>
	 * <li>in enum fields, the method will accept any object whose toString() returns a value equal to the Enum's toString()
	 * <li>with primitives fields, the method will accept any object whose toString() returns a parseable value for the assigned primitive
	 * </ul>
	 */
	public void setValueFromAny(Object value) throws IllegalAccessException
	{
		if (value == null)
		{
			if (field == null)
				return; // nullifying a non-existing field isn't considered an error
			setValue(null);
			return;
		}

		if (field == null)
			throw new RuntimeException("Object containing " + fieldPath + " doesn't exist, use ensurePath=true");

		try
		{
			Class<?> assignFrom = value.getClass();
			Class<?> assignTo = getType();
			if (assignTo.isAssignableFrom(assignFrom))
				setValue(value);
			else
			{
				String valueAsStr = value.toString();
				if (!valueAsStr.isEmpty())
				{
					if (assignTo.isEnum())
						applyEnum(assignTo, valueAsStr);
					else
					{
						Object primitive = strToPrimitive(valueAsStr, assignTo);
						if (primitive != null)
							setValue(primitive);
						else
							throw new RuntimeException("Couldn't set field " + fieldPath + ": can't assign " + assignFrom.getName()
									+ " to " + assignTo.getName());
					}
				}
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException("couldn't assign to '" + fieldPath + "' value of " + value, e);
		}
	}

	private void applyEnum(Class<?> enumType, String enumValue) throws IllegalAccessException
	{
		for (Object e : enumType.getEnumConstants())
		{
			if (e.toString().equals(enumValue))
			{
				setValue(e);
				return;
			}
		}
		throw new RuntimeException("Value '" + enumValue + "' couldn't be assigned to enum " + fieldPath + " (of type "
				+ enumType.getName() + ")");
	}

	private Object strToPrimitive(String value, Class<?> clz)
	{
		if (Integer.class == clz || int.class == clz)
			return Integer.parseInt(value);
		if (Double.class == clz || double.class == clz)
			return Double.parseDouble(value);
		if (Long.class == clz || long.class == clz)
			return Long.parseLong(value);
		if (Float.class == clz || float.class == clz)
			return Float.parseFloat(value);
		if (Boolean.class == clz || boolean.class == clz)
			return Boolean.parseBoolean(value);
		if (Byte.class == clz || byte.class == clz)
			return Byte.parseByte(value);
		if (Short.class == clz || short.class == clz)
			return Short.parseShort(value);
		return null;
	}
}