package net.sf.hajdbc;

import java.sql.SQLException;

import net.sf.hajdbc.codec.Decoder;

public interface Credentials
{
	String getUser();

	String decodePassword(Decoder decoder) throws SQLException;
}
