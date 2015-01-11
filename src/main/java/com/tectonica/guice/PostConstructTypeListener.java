/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-commons
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.guice;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.annotation.PostConstruct;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

public class PostConstructTypeListener implements TypeListener
{
	private final String packagePrefix;

	public PostConstructTypeListener(String packagePrefix)
	{
		this.packagePrefix = packagePrefix;
	}

	@Override
	public <I> void hear(final TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter)
	{
		Class<? super I> clz = typeLiteral.getRawType();
		if (packagePrefix != null && !clz.getName().startsWith(packagePrefix))
			return;

		final Method method = getPostConstructMethod(clz);
		if (method != null)
		{
			typeEncounter.register(new InjectionListener<I>()
			{
				@Override
				public void afterInjection(Object i)
				{
					try
					{
						// call the @PostConstruct annotated method after all dependencies have been injected
						method.invoke(i);
					}
					catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
					{
						throw new RuntimeException(e);
					}
				}
			});
		}
	}

	/**
	 * checks whether the provided class, or one of its super-classes, has a method with the {@code PostConstruct} annotation. the method
	 * may be public, protected, package-private or private. if it's inaccessible by Java rules, this method will also make
	 * it accessible before returning it.
	 * 
	 * @return
	 *         the method that meets all requirements, or null if none found
	 */
	private Method getPostConstructMethod(Class<?> clz)
	{
		for (Method method : clz.getDeclaredMethods())
		{
			if (method.getAnnotation(PostConstruct.class) != null && isPostConstructEligible(method))
			{
				method.setAccessible(true);
				return method;
			}
		}
		Class<?> superClz = clz.getSuperclass();
		return (superClz == Object.class || superClz == null) ? null : getPostConstructMethod(superClz);
	}

	/**
	 * apply restrictions as defined in the <a
	 * href="http://docs.oracle.com/javaee/5/api/javax/annotation/PostConstruct.html">JavaEE specifications</a>
	 */
	private boolean isPostConstructEligible(final Method method)
	{
		return (method.getReturnType() == void.class) && (method.getParameterTypes().length == 0)
				&& (method.getExceptionTypes().length == 0);
	}
}
