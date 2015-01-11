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

import com.tectonica.guice.GuiceJsfModule;
import com.tectonica.guice.GuiceListener;

/**
 * Servlet-listener for setting Guice as the dependency-injection engine for JSF ManagedBeans. Intended to be used with
 * {@link GuiceJsfInjector}.
 * <p>
 * To use it, first make sure to include the following dependency in your {@code pom.xml}:
 * 
 * <pre>
 * &lt;dependency&gt;
 *    &lt;groupId&gt;com.google.inject.extensions&lt;/groupId&gt;
 *    &lt;artifactId&gt;guice-servlet&lt;/artifactId&gt;
 * &lt;/dependency&gt;
 * </pre>
 * 
 * Then, if no extra-bindings are needed, you can simply register the listener with {@code web.xml}:
 * 
 * <pre>
 * &lt;listener&gt;
 *    &lt;listener-class&gt;com.tectonica.guice.GuiceJsfListener&lt;/listener-class&gt;
 * &lt;/listener&gt;
 * </pre>
 * 
 * If you need additional bindings, please extend {@link GuiceListener} by providing a constructor that passes a subclass of
 * {@link GuiceJsfModule}.
 * 
 * @author Zach Melamed
 */
public class GuiceJsfListener extends GuiceListener
{
	public GuiceJsfListener()
	{
		super(new GuiceJsfModule());
	}
}