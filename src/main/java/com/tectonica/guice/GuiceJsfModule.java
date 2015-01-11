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