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

package com.tectonica.gae;

import javax.faces.context.FacesContext;
import javax.faces.event.PhaseEvent;
import javax.faces.event.PhaseId;
import javax.faces.event.PhaseListener;

/**
 * PhaseListener to ensure the HttpSession data is written to the datastore.
 * <p>
 * To use, add the following to your {@code faces-config.xml}:
 * 
 * <pre>
 * &lt;lifecycle&gt;
 *    &lt;phase-listener&gt;com.tectonica.gae.GaePhaseListener&lt;/phase-listener&gt;
 * &lt;/lifecycle&gt;
 * </pre>
 * 
 * If properly configured, an application deployed on GAE platform can leverage HttpSessions to store data between requests. Session data is
 * stored in the datastore and memcache is also used for speed. Session data is persisted at the <strong>end</strong> of the request.
 * <p>
 * The App Engine session implementation will not recognize if properties of objects stored in the session are changed which is why we have
 * this <code>PhaseListener</code> which will modify a session attribute with the current date and time (in milliseconds) at the end of
 * every phase.
 * 
 * @see https://developers.google.com/appengine/docs/java/config/appconfig#Java_appengine_web_xml_Enabling_sessions
 * @see http://stackoverflow.com/questions/19259457/session-lost-in-google-app-engine-using-jsf
 */
public class GaePhaseListener implements PhaseListener
{
	private static final long serialVersionUID = 1L;

	@Override
	public void afterPhase(PhaseEvent event)
	{
		FacesContext.getCurrentInstance().getExternalContext().getSessionMap().put("CURRENT_TIME", System.currentTimeMillis());
	}

	@Override
	public void beforePhase(PhaseEvent event)
	{}

	@Override
	public PhaseId getPhaseId()
	{
		return PhaseId.ANY_PHASE;
	}
}
