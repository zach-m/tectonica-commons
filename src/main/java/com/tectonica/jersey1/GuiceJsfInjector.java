package com.tectonica.jersey1;

import javax.faces.bean.ManagedBean;

import com.sun.faces.spi.InjectionProviderException;
import com.sun.faces.vendor.WebContainerInjectionProvider;

/**
 * An optional injector for Mojarra-based applications where dependency injection is required into the {@code @ManagedBean}s. It extends
 * {@link WebContainerInjectionProvider}, which normally handles invocations of {@code @PostConstruct} and {@code @PreDestroy}, by also
 * adding dependency-injection for {@code @ManagedBean}s using the Guice injector created in {@link GuiceRestListener}.
 * <p>
 * To use, add the following paragraph to {@code web.xml} alongside your other JSF configuration:
 * 
 * <pre>
 * &lt;context-param&gt;
 *    &lt;param-name&gt;com.sun.faces.injectionProvider&lt;/param-name&gt;
 *    &lt;param-value&gt;com.tectonica.jersey1.GuiceJsfInjector&lt;/param-value&gt;
 * &lt;/context-param&gt;
 * </pre>
 * 
 * @author Zach Melamed
 */
public class GuiceJsfInjector extends WebContainerInjectionProvider
{
	@Override
	public void inject(Object managedBean) throws InjectionProviderException
	{
		// we only want to inject into @ManagedBean annotated classes
		boolean isManagedBean = managedBean.getClass().getAnnotation(ManagedBean.class) != null;
		if (isManagedBean)
			GuiceRestListener.injectMembers(managedBean);
	}

	@Override
	public void invokePostConstruct(Object managedBean) throws InjectionProviderException
	{
		// @PostConstruct is already handled by us for classes annotated with @ManagedBean
		boolean isManagedBean = managedBean.getClass().getAnnotation(ManagedBean.class) != null;
		if (!isManagedBean)
			super.invokePostConstruct(managedBean);
	}

	@Override
	public void invokePreDestroy(Object managedBean) throws InjectionProviderException
	{
		super.invokePreDestroy(managedBean);
	}
}
