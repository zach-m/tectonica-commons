package com.tectonica.util;

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

import com.tectonica.util.SearchReplaceReader.TokenResolver;

public abstract class StaticServlet extends HttpServlet
{
	private static final long serialVersionUID = 1L;

	public static interface BasicAuthorizer
	{
		public boolean isAuthorized(String username, String password);
	}

	protected abstract String getLocalPath(String servletPath, String uriPath);

	protected abstract BasicAuthorizer getBasicAuthorizer();

	protected abstract TokenResolver getTokenResolver(String localPath);

//	private TokenResolver createResolver(ConfigInfo config)
//	{
//		Map<String, String> tokens = new HashMap<String, String>();
//
//		tokens.put("name", "zach");
//		tokens.put("email", "zach@tectonica.co.il");
//
//		return new MapTokenResolver(tokens);
//	}

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

	protected boolean checkAuthorization(HttpServletRequest request, HttpServletResponse response)
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

	protected String getMimeType(HttpServletResponse response, String path)
	{
		String mimeType = getServletContext().getMimeType(path);
		if (mimeType == null)
			mimeType = "application/octet-stream";
		return mimeType;
	}

	protected void setCustomHeaders(HttpServletResponse response)
	{
//		response.setHeader("Cache-Control", "max-age=900,must-revalidate"); // 15-minutes
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
