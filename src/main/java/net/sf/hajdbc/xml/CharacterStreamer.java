package net.sf.hajdbc.xml;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public interface CharacterStreamer
{
	Reader getReader() throws IOException;
	
	Writer getWriter() throws IOException;
}
