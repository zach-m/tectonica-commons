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

import javax.faces.bean.ManagedBean;

import com.sun.faces.spi.InjectionProviderException;
import com.sun.faces.vendor.WebContainerInjectionProvider;

/**
 * An optional injector for Mojarra-based applications where dependency injection is required into the {@code @ManagedBean}s. It extends
 * {@link WebContainerInjectionProvider}, which normally handles invocations of {@code @PostConstruct} and {@code @PreDestroy}, by also
 * adding dependency-injection for {@code @ManagedBean}s using the Guice injector created in {@link GuiceListener}. This creator, by the
 * way, also handles {@code @PostConstruct} methods, so we make sure to avoid double invocation here.
 * <p>
 * To use, add the following paragraph to {@code web.xml} alongside your other JSF configuration:
 * 
 * <pre>
 * &lt;context-param&gt;
 *    &lt;param-name&gt;com.sun.faces.injectionProvider&lt;/param-name&gt;
 *    &lt;param-value&gt;com.tectonica.guice.GuiceJsfInjector&lt;/param-value&gt;
 * &lt;/context-param&gt;
 * </pre>
 * 
 * <b>NOTE:</b> make sure your {@link GuiceListener}-subclass is an active listener in the {@code web.xml}, or NullPointerExceptions
 * will be thrown
 * <p>
 * 
 * @author Zach Melamed
 */
public class GuiceJsfInjector extends WebContainerInjectionProvider
{
	@Override
	public void inject(Object managedBean) throws InjectionProviderException
	{
		if (isToBeInjectedByGuice(managedBean))
			GuiceListener.injectMembers(managedBean);
	}

	/**
	 * as an arbitrary choice, the choice here is to inject only into {@code @ManagedBean} instances, so that other classes - not written by
	 * us - wouldn't be injected too. This choice could be altered.
	 * 
	 * @param managedBean
	 * @return
	 */
	private boolean isToBeInjectedByGuice(Object managedBean)
	{
		return managedBean.getClass().getAnnotation(ManagedBean.class) != null;
	}

	@Override
	public void invokePostConstruct(Object managedBean) throws InjectionProviderException
	{
		// @PostConstruct is already handled in classes we injected
		if (!isToBeInjectedByGuice(managedBean))
			super.invokePostConstruct(managedBean);
	}

	@Override
	public void invokePreDestroy(Object managedBean) throws InjectionProviderException
	{
		super.invokePreDestroy(managedBean);
	}
}
