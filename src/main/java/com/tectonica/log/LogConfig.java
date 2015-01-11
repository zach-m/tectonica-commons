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

package com.tectonica.log;

import java.io.InputStream;
import java.util.logging.LogManager;

/**
 * Configures J.U.L by dynamically loading a <code>logging.properties</code> file from the classpath. To use:
 * <ol>
 * <li>put <code>logging.properties</code> in the <code>src/main/resources</code> folder
 * <li>add the following to your JVM arguments:
 * 
 * <pre>
 * -Djava.util.logging.config.class=com.tectonica.log.LogConfig
 * </pre>
 * 
 * </ol>
 * 
 * @author Zach Melamed
 */
public class LogConfig
{
	public LogConfig() throws Exception
	{
		try (InputStream ins = ClassLoader.getSystemResourceAsStream("logging.properties"))
		{
			LogManager.getLogManager().readConfiguration(ins);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
