package com.tectonica.util;

import java.lang.reflect.Field;

public class REF
{
	/**
	 * assigns all the non-null fields of {@code overlay} into {@code base}
	 */
	public static <T> T overrideFields(T base, T overlay)
	{
		overrideFields(base.getClass(), base, overlay);
		return base;
	}

	private static void overrideFields(Class<?> clz, Object base, Object overlay)
	{
		try
		{
			for (Field field : clz.getDeclaredFields())
			{
				field.setAccessible(true);
				Object override = field.get(overlay);
				if (override != null)
					field.set(base, override);
			}
			Class<?> superclz = clz.getSuperclass();
			if (!Object.class.equals(superclz))
				overrideFields(superclz, base, overlay);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

//	@Getter
//	@Setter
//	public static class Animal
//	{
//		private String name;
//		private Integer age;
//	}
//
//	@Getter
//	@Setter
//	public static class Dog extends Animal
//	{
//		private Double tail;
//		private String bark;
//
//		@Override
//		public String toString()
//		{
//			return "Dog [getTail()=" + getTail() + ", getBark()=" + getBark() + ", getName()=" + getName() + ", getAge()=" + getAge() + "]";
//		}
//	}
//
//	public static void main(String[] args)
//	{
//		Dog d = new Dog();
//		d.setName("ralph");
//		d.setAge(5);
//		d.setBark("Arf");
//
//		Dog up = new Dog();
//		up.setAge(6);
//		up.setTail(6.6);
//
//		System.out.println(overrideFields(d, up));
//	}
}
