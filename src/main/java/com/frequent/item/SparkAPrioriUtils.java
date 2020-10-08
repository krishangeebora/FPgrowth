package com.frequent.item;
public class SparkAPrioriUtils {
	
	public static String generatePairs(int[] items) {
		
		if ( items == null || items.length <= 1 ) {
			return null;
		}
		
		StringBuilder sb = new StringBuilder();
		for ( int i = 0; i < items.length-1; i ++ ) {
			for ( int j = i+1; j < items.length; j ++ ) {
				sb.append(items[i]).append("-").append(items[j]).append(",");
			}
		}
		sb.setLength(sb.length() - 1);
		// System.out.println(sb);
		return sb.toString();
	}

}
