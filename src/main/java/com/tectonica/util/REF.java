package com.tectonica.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

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
				int mods = field.getModifiers();
				if (!Modifier.isStatic(mods) && !Modifier.isFinal(mods))
				{
					field.setAccessible(true);
					Object override = field.get(overlay);
					if (override != null)
						field.set(base, override);
				}
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

//	public static class Animal
//	{
//		private String name;
//		private int age;
//
//		public String getName()
//		{
//			return name;
//		}
//
//		public void setName(String name)
//		{
//			this.name = name;
//		}
//
//		public int getAge()
//		{
//			return age;
//		}
//
//		public void setAge(int age)
//		{
//			this.age = age;
//		}
//	}
//
//	public static class Dog extends Animal
//	{
//		private Double tail;
//		private String bark;
//
//		public Double getTail()
//		{
//			return tail;
//		}
//
//		public void setTail(Double tail)
//		{
//			this.tail = tail;
//		}
//
//		public String getBark()
//		{
//			return bark;
//		}
//
//		public void setBark(String bark)
//		{
//			this.bark = bark;
//		}
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
