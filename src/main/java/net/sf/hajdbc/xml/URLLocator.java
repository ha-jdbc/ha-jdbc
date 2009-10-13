package net.sf.hajdbc.xml;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;

public class URLLocator implements Locator
{
	private final URL url;
	
	public URLLocator(URL url)
	{
		this.url = url;
	}
	
	@Override
	public Reader getReader() throws IOException
	{
		return new BufferedReader(new InputStreamReader(this.url.openConnection().getInputStream()));
	}

	@Override
	public Writer getWriter() throws IOException
	{
		return new BufferedWriter(this.url.getProtocol().equals("file") ? new FileWriter(new File(this.url.getPath())) : new OutputStreamWriter(this.url.openConnection().getOutputStream()));
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.url.toString();
	}
}
