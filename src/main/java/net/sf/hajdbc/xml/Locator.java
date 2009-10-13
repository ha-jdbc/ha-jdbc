package net.sf.hajdbc.xml;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public interface Locator
{
	Reader getReader() throws IOException;
	
	Writer getWriter() throws IOException;
}
