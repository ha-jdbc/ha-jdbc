package net.sf.hajdbc.codec;

import java.sql.SQLException;

public interface Decoder
{
	String decode(String value) throws SQLException;
}
