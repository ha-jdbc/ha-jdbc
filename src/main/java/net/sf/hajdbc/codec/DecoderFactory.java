package net.sf.hajdbc.codec;

import java.sql.SQLException;

public interface DecoderFactory
{
	Decoder createDecoder(String clusterId) throws SQLException;
}
