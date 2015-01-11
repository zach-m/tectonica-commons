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

package com.tectonica.jee5;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.matcher.Matchers;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.tectonica.guice.PostConstructTypeListener;

public class GuiceRestModule extends ServletModule
{
	@Override
	protected void configureServlets()
	{
		// add support for the @PostConstruct annotation for Guice-injected objects
		// if you choose to remove it, also modify GuiceJsfInjector.invokePostConstruct()
		bindListener(Matchers.any(), new PostConstructTypeListener(null));

		doCustomBinds();

		bindJaxrsResources();

		// configure Jersey: use Jackson + CORS-filter
		Map<String, String> initParams = new HashMap<>();
		if (isUseJackson())
			initParams.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
		String reponseFilters = getResponseFilters();
		if (reponseFilters != null)
			initParams.put("com.sun.jersey.spi.container.ContainerResponseFilters", reponseFilters);

		doCustomJerseyParameters(initParams);

		// route all requests through Guice
		serve(getServingUrl()).with(GuiceContainer.class, initParams);

		doCustomServing();
	}

	/**
	 * override this to specify the URL-pattern of the REST service
	 */
	protected String getServingUrl()
	{
		return "/*";
	}

	/**
	 * override this to specify a root-package under which all your JAX-RS annotated class are located
	 */
	protected String getRootPackage()
	{
		return null;
	}

	/**
	 * bind JAX-RS annotated classes to be served through Guice. based on a recursive class scanning that starts from the package
	 * returned by {@link #getRootPackage()}. override this if you wish to avoid the scanning and bind your classes explicitly
	 */
	protected void bindJaxrsResources()
	{
		String rootPackage = getRootPackage();
		if (rootPackage == null)
			throw new NullPointerException(
					"to scan for JAX-RS annotated classes, either override getRootPackage() or bindJaxrsResources()");

		ResourceConfig rc = new PackagesResourceConfig(rootPackage);
		for (Class<?> resource : rc.getClasses())
			bind(resource);
	}

	/**
	 * override this to return a (comma-delimited) list of Jersey's ContainerResponseFilters. By default returns the {@link CorsFilter}.
	 */
	protected String getResponseFilters()
	{
		return CorsFilter.class.getName();
	}

	/**
	 * override this to avoid usage of Jackson
	 */
	protected boolean isUseJackson()
	{
		return true;
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

	/**
	 * override to add additional Guice configuration. For example, to have non-REST servlets served through Guice, use:
	 * 
	 * <pre>
	 * serve(&quot;/my/*&quot;).with(MyServlet.class);
	 * </pre>
	 */
	protected void doCustomServing()
	{}

	/**
	 * override to change the context-parameters passed to Jersey's servlet. For example:
	 * 
	 * <pre>
	 * initParams.put(&quot;com.sun.jersey.config.feature.Trace&quot;, &quot;true&quot;);
	 * </Per>
	 */
	protected void doCustomJerseyParameters(Map<String, String> initParams)
	{}
}