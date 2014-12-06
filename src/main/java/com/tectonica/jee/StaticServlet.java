package com.tectonica.jee;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;

import com.tectonica.util.SearchReplaceReader;
import com.tectonica.util.SearchReplaceReader.TokenResolver;

public abstract class StaticServlet extends HttpServlet
{
	private static final long serialVersionUID = 1L;

	public static interface BasicAuthorizer
	{
		public boolean isAuthorized(String username, String password);
	}

	/**
	 * <p>
	 * Returns the exact path, within the WAR file, of the resource to return (given a requested URL, indicated in the passed request
	 * parameter). The path should be relative to the 'webapp' folder and start with a '/'.
	 * <p>
	 * If your goal is to serve resources without changing your tree structure (which will later allow you to activate/deactivate this
	 * servlet without having to change any URLs), first make sure to have a mapping like this in {@code web.xml} (assuming, of course, that
	 * the resources are under {@code /webapp/apidocs}):
	 * 
	 * <pre>
	 * &lt;servlet-mapping>
	 *     &lt;url-pattern>/apidocs/*&lt;/url-pattern>
	 * &lt;/servlet-mapping>
	 * </pre>
	 * 
	 * then, when overriding this function, simply return:
	 * 
	 * <pre>
	 * return request.getServletPath() + request.getPathInfo();
	 * </pre>
	 * 
	 * This is probably the most typical scenario, certainly when the use-case is restricting access to the static content (but not the only
	 * one possible).
	 * <p>
	 * <b>NOTE:</b> returning null will result-in an HTTP-400 response.
	 * 
	 * @param request
	 *            the request itself, from which the request URL can be obtained. one may also use it to set attributes for later use
	 */
	protected abstract String getLocalPath(HttpServletRequest request);

	/**
	 * Returns an (optionally null) search-replace resolver to apply on textual content before serving them. This resolver may be created
	 * like that:
	 * 
	 * <pre>
	 * Map&lt;String, String&gt; tokens = new HashMap&lt;String, String&gt;();
	 * 
	 * tokens.put(&quot;NAME&quot;, &quot;zach&quot;);
	 * tokens.put(&quot;EMAIL&quot;, &quot;zach@tectonica.co.il&quot;);
	 * 
	 * return new MapTokenResolver(tokens);
	 * </pre>
	 * 
	 * When such resolver is applied, any occurrence of <b><code>${NAME}</code></b> and <b><code>${EMAIL}</code></b> will be replaced with
	 * the corresponding values listed above.
	 */
	protected TokenResolver getTokenResolver(String localPath, HttpServletRequest request)
	{
		return null;
	}

	/**
	 * Returns an (optionally null) authorizer to use before serving resources. If you wish to apply a more sophisticated authorization
	 * scheme, you need to override {@link #checkAuthorization(HttpServletRequest, HttpServletResponse)}.
	 */
	protected BasicAuthorizer getBasicAuthorizer()
	{
		return null;
	}

	/**
	 * <p>
	 * Performs authorization check before serving a resource. The default behavior is to apply an HTTP BASIC AUTHORIZATION strategy using a
	 * user-supplied implementation of {@link BasicAuthorizer}. If the user doesn't supply such implementation in
	 * {@link #getBasicAuthorizer()}, the resource is assumed cleared to serve.
	 * <p>
	 * an example for using Google App Engine's Accounts API is as follows:
	 * 
	 * <pre>
	 * &#064;Override
	 * protected boolean checkAuthorization(HttpServletRequest request, HttpServletResponse response) throws IOException
	 * {
	 * 	boolean authorized = false;
	 * 
	 * 	UserService userService = UserServiceFactory.getUserService();
	 * 	if (userService.isUserLoggedIn())
	 * 		authorized = ApiProxy.getCurrentEnvironment().getEmail().endsWith(&quot;@tectonica.co.il&quot;);
	 * 
	 * 	if (!authorized)
	 * 		response.sendRedirect(userService.createLoginURL(request.getRequestURI()));
	 * 
	 * 	return authorized;
	 * }
	 * </pre>
	 * 
	 * @return
	 *         whether or not the resource is authorized for serving. if {@code false}, the main {@code doGet()} execution stops.
	 */
	protected boolean checkAuthorization(HttpServletRequest request, HttpServletResponse response) throws IOException
	{
		BasicAuthorizer basicAuthorizer = getBasicAuthorizer();
		if (basicAuthorizer == null)
			return true;

		String authHeader = request.getHeader("Authorization");
		if (authHeader != null && authHeader.startsWith("Basic "))
		{
			String[] values = new String(DatatypeConverter.parseBase64Binary(authHeader.substring("Basic ".length()))).split(":");
			return (values != null) && (values.length > 1) && (basicAuthorizer.isAuthorized(values[0], values[1]));
		}

		response.setHeader("WWW-Authenticate", String.format("Basic realm=\"%s\"", request.getHeader("Host")));
		response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		return false;
	}

	/**
	 * Override if you wish to modify the response headers before the resource is served. This could be a good place to add client-side
	 * caching for the resource, like that:
	 * 
	 * <pre>
	 * response.setHeader(&quot;Cache-Control&quot;, &quot;max-age=900,must-revalidate&quot;); // 15-minutes
	 * </pre>
	 */
	protected void setCustomHeaders(HttpServletRequest request, HttpServletResponse response)
	{}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		String localPath = getLocalPath(request);
		if (localPath == null)
		{
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		InputStream is = getServletContext().getResourceAsStream(localPath);
		if (is == null)
		{
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		if (!checkAuthorization(request, response))
			return;

		String mimeType = getMimeType(localPath, request);
		response.setContentType(mimeType);

		setCustomHeaders(request, response);

		ServletOutputStream os = response.getOutputStream();

		if (!(mimeType.startsWith("text/") && writeManipulatedChars(is, os, localPath, request)))
			writeBytes(is, os); // i.e. return as-is

		os.close();
	}

	protected String getMimeType(String path, HttpServletRequest request)
	{
		String mimeType = getServletContext().getMimeType(path);
		if (mimeType == null)
			mimeType = "application/octet-stream";
		return mimeType;
	}

	private boolean writeManipulatedChars(InputStream is, OutputStream os, String localPath, HttpServletRequest request)
			throws IOException
	{
		TokenResolver tokenResolver = getTokenResolver(localPath, request);
		if (tokenResolver == null)
			return false;

		SearchReplaceReader reader = new SearchReplaceReader(new InputStreamReader(is), tokenResolver);
		OutputStreamWriter writer = new OutputStreamWriter(os);
		writeChars(reader, writer);
		return true;
	}

	protected void writeBytes(InputStream in, OutputStream out) throws IOException
	{
		int read;
		final byte[] data = new byte[8192];
		while ((read = in.read(data)) != -1)
			out.write(data, 0, read);
		out.flush();
	}

	protected void writeChars(Reader in, Writer out) throws IOException
	{
		int read;
		final char[] data = new char[8192];
		while ((read = in.read(data)) != -1)
			out.write(data, 0, read);
		out.flush();
	}
}
