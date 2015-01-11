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

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

/**
 * Jersey filter for adding CORS headers to the response of the REST APIs where needed
 * 
 * @author Zach Melamed
 */
public class CorsFilter implements ContainerResponseFilter
{
	@Override
	public ContainerResponse filter(ContainerRequest request, ContainerResponse response)
	{
		if (!isCorsNeeded(request))
			return response;

		MultivaluedMap<String, Object> headers = response.getHttpHeaders();

		headers.add("Access-Control-Allow-Origin", "*");
		headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

		String acrh = request.getHeaderValue("Access-Control-Request-Headers");
		if (acrh != null && !acrh.isEmpty())
			headers.add("Access-Control-Allow-Headers", acrh);

		return response;
	}

	/**
	 * returns whether or not a given request needs CORS header in its response. For example:
	 * 
	 * <pre>
	 * return request.getPath().startsWith(&quot;debug&quot;);
	 * </pre>
	 */
	protected boolean isCorsNeeded(ContainerRequest request)
	{
		return true;
	}
}
