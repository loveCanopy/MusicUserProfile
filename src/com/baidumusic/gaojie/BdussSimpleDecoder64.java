package com.baidumusic.gaojie;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import javax.xml.bind.DatatypeConverter;

public class BdussSimpleDecoder64 {
	public static long decode64(String bduss) {
		if (bduss == null || bduss.length() != 192) {
			return 0L;
		}
		// get mod & key
		int intMod = 0;
		int len = bduss.length();
		for (int i = 0; i < len; i++) {
			intMod += i;
			intMod += bduss.charAt(i);
		}
		intMod = intMod % 2 + 1;
		bduss = bduss.substring(len - intMod) + bduss.substring(0, len - intMod);

		byte[] bstr = DatatypeConverter.parseBase64Binary(bduss.replace('-', '+').replace('~', '/'));

		ByteBuffer bufHigh = ByteBuffer.wrap(bstr, 60, 4);
		bufHigh.order(ByteOrder.LITTLE_ENDIAN);

		ByteBuffer bufLow = ByteBuffer.wrap(bstr, 68, 4);
		bufLow.order(ByteOrder.LITTLE_ENDIAN);

		return ((long) bufHigh.getInt() << 32 & 0xffffffff00000000L) | ((long) bufLow.getInt() & 0x00000000ffffffffL);
	}
}