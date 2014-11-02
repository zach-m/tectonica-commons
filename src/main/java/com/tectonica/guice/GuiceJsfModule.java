package com.tectonica.guice;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

public class GuiceJsfModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		// add support for the @PostConstruct annotation for Guice-injected objects
		// if you choose to remove it, also modify GuiceJsfInjector.invokePostConstruct()
		bindListener(Matchers.any(), new PostConstructTypeListener(null));

		doCustomBinds();
	}

	/**
	 * override to perform application-logic bindings, typically between interfaces and concrete implementations. For example:
	 * 
	 * <pre>
	 * bind(MyIntf.class).to(MyImpl.class);
	 * </per>
	 */
	protected void doCustomBinds()
	{}
}