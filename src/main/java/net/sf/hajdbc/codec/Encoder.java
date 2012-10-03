package net.sf.hajdbc.codec;

import java.sql.SQLException;

public interface Encoder
{
	String encode(String value) throws SQLException;
}
