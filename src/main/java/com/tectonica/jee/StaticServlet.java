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
	 * Returns the exact path, within the WAR file, of the resource to return (given an requested HTTP url, passed in two parameters for
	 * this function). The path is relative to the 'webapp' folder.
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
	 * return servletPath + uriPath;
	 * </pre>
	 * 
	 * This is probably the most typical scenario, certainly when the use-case is restricting access to the static content, but not the only
	 * one possible.
	 * 
	 * @param servletPath
	 *            the mapped part of the resource URL, as indicated in the {@code <servlet-mapping>} paragraph
	 * @param uriPath
	 *            the remaining part of the resource URL, typically ending with a file extension
	 */
	protected abstract String getLocalPath(String servletPath, String uriPath);

	/**
	 * Returns an (optionally null) search-replace resolver to apply on textual content before serving them. This resolver may be created
	 * like that:
	 * 
	 * <pre>
	 * Map&lt;String, String&gt; tokens = new HashMap&lt;String, String&gt;();
	 * 
	 * tokens.put(&quot;name&quot;, &quot;zach&quot;);
	 * tokens.put(&quot;email&quot;, &quot;zach@tectonica.co.il&quot;);
	 * 
	 * return new MapTokenResolver(tokens);
	 * </pre>
	 * 
	 * When such resolver is applied, any occurrence of {@code $name} and {@code $email} will be replaced with the corresponding values
	 * listed above.
	 */
	protected TokenResolver getTokenResolver(String localPath)
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
	 *     boolean authorized = false;
	 * 
	 *     UserService userService = UserServiceFactory.getUserService();
	 *     if (userService.isUserLoggedIn())
	 *         authorized = ApiProxy.getCurrentEnvironment().getEmail().endsWith(&quot;@tectonica.co.il&quot;);
	 * 
	 *     if (!authorized)
	 *         response.sendRedirect(userService.createLoginURL(request.getRequestURI()));
	 * 
	 *     return authorized;
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
	protected void setCustomHeaders(HttpServletResponse response)
	{}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		String uriPath = request.getPathInfo();
		String servletPath = request.getServletPath();
		String localPath = getLocalPath(servletPath, uriPath);
		InputStream is = getServletContext().getResourceAsStream(localPath);
		if (is == null)
		{
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		if (!checkAuthorization(request, response))
			return;

		String mimeType = getMimeType(response, uriPath);
		response.setContentType(mimeType);

		setCustomHeaders(response);

		ServletOutputStream os = response.getOutputStream();

		if (!returnManipulatedText(is, os, localPath, mimeType))
			writeBinary(is, os); // i.e. return as-is

		os.close();
	}

	protected String getMimeType(HttpServletResponse response, String path)
	{
		String mimeType = getServletContext().getMimeType(path);
		if (mimeType == null)
			mimeType = "application/octet-stream";
		return mimeType;
	}

	private boolean returnManipulatedText(InputStream is, OutputStream os, String localPath, String mimeType) throws IOException
	{
		TokenResolver tokenResolver = getTokenResolver(localPath);
		if ((tokenResolver != null) && mimeType.startsWith("text/"))
		{
			SearchReplaceReader reader = new SearchReplaceReader(new InputStreamReader(is), tokenResolver);
			OutputStreamWriter writer = new OutputStreamWriter(os);
			writeText(reader, writer);
			return true;
		}
		return false;
	}

	protected void writeBinary(InputStream in, OutputStream out) throws IOException
	{
		int read;
		final byte[] data = new byte[8192];
		while ((read = in.read(data)) != -1)
			out.write(data, 0, read);
	}

	protected void writeText(Reader in, Writer out) throws IOException
	{
		int read;
		final char[] data = new char[8192];
		while ((read = in.read(data)) != -1)
			out.write(data, 0, read);
	}
}
