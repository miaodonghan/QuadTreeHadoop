package io;

import java.nio.ByteBuffer;

import quadIndex.Point;
import quadIndex.Polygon;
import quadIndex.Rectangle;
import quadIndex.SpatialObj;

public class InputParser {

	public static SpatialObj getObjFromLine(String line) {
		if (line.contains("EMPTY"))
			return null;

		int pos1 = line.indexOf('(');
		int pos2 = line.indexOf(')');
		line = line.substring(pos1 + 1, pos2 - pos1 - 1);
		line.replace(',' , ' '  );
		String delims = "[ ]+";
		String[] strings = line.split(delims);
		java.util.List<Float> nums = new java.util.ArrayList<Float>();
		for (int i = 1; i < strings.length; i++)
			nums.add(Float.parseFloat(strings[i]));

		int npoints = strings.length / 2;

		/*
		 * if (npoints == 1) { // Point object = new Point(nums.get(1),
		 * nums.get(2)); } else if (npoints == 4) { // Rectangle // suppose
		 * order is first --- second // | | // | | // fourth--- third float
		 * width = Math.abs(nums.get(2) - nums.get(0)); // x2-x1 float height =
		 * Math.abs(nums.get(1) - nums.get(3)); // y1-y4
		 * 
		 * object = new Rectangle(nums.get(1), nums.get(2), width, height); }
		 * else if (npoints >= 6) { // Polygon
		 */
		float[] xs = new float[npoints];
		float[] ys = new float[npoints];
		for (int i = 0; i < npoints; i++) {
			xs[i] = nums.get(2 * i);
			ys[i] = nums.get(2 * i + 1);
		}
		SpatialObj object = new Polygon(xs, ys, npoints);

		/*
		 * } else { System.err.println("No such object"); return null; }
		 */

		return object;
	}

	public static SpatialObj getObjFromBytes(byte[] bytes) {
		int len = bytes.length;
		ByteBuffer wrapped = ByteBuffer.wrap(bytes);
		int num = len / 4;

		if (num < 0) {
			return null;
		} else if (num == 2) {
			float x = wrapped.getFloat();
			float y = wrapped.getFloat();
			return new Point(x, y);
		} else if (num == 4) {
			float x = wrapped.getFloat();
			float y = wrapped.getFloat();
			float w = wrapped.getFloat();
			float h = wrapped.getFloat();
			return new Rectangle(x, y, w, h);
		} else {
			int npoints = wrapped.getInt();
			float x[] = new float[npoints];
			float y[] = new float[npoints];
			for (int i = 0; i < npoints; i++) {
				x[i] = wrapped.getFloat();
			}
			for (int i = 0; i < npoints; i++) {
				y[i] = wrapped.getFloat();
			}
			return new Polygon(x, y, npoints);
		}
	}
}
