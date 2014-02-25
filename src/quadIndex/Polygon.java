package quadIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/*
 * A helper Class as a substitude for java.awt.polygon
 * Author: Li Wang
 */
public class Polygon extends SpatialObj implements Writable {
	private static final int initialSize = 8;

	public Polygon() {
		xs = new double[initialSize];
		ys = new double[initialSize];
		npoints = 0;
	}

	public Polygon(double[] x, double[] y, int n) {
		xs = x;
		ys = y;
		npoints = n;
	}

	private double[] copyArray(double[] small, double[] large) {
		for (int i = 0; i < small.length; i++) {
			large[i] = small[i];
		}
		return large;
	}

	public void print() {
		for (int i = 0; i < npoints; i++) {
			System.out.print(xs[i] + " " + ys[i] + ", ");
		}
		System.out.println();
	}

	public void addPoint(double x, double y) {
		if (npoints == xs.length) {
			double[] new_xs = new double[xs.length * 2];
			double[] new_ys = new double[ys.length * 2];
			xs = copyArray(xs, new_xs);
			ys = copyArray(ys, new_ys);
		}
		npoints++;
		xs[npoints - 1] = x;
		ys[npoints - 1] = y;
	}

	public boolean intersects(int x, int y, double width, double height) {
		Rectangle rect = new Rectangle(x, y, (int) width, (int) height);
		Rectangle a_rect = this.getMBR();
		return rect.intersects(a_rect);
	}

	@Override
	public boolean intersects(Rectangle rect) {
		Rectangle a_rect = this.getMBR();
		return rect.intersects(a_rect);
	}

	/*
	 * @Override public boolean intersects(double x, double y, double width, double
	 * height) { // TODO Auto-generated method stub return false; }
	 * 
	 * @Override public boolean intersects(double x, double y, double radius) { //
	 * TODO Auto-generated method stub return false; }
	 */

	public Rectangle getMBR() {
		double low = Double.POSITIVE_INFINITY;
		double up = Double.NEGATIVE_INFINITY;

		for (int i = 0; i < xs.length; i++) {
			if (xs[i] < low) {
				low = xs[i];
			}
			if (xs[i] > up) {
				up = xs[i];
			}
		}
		double lowerbound_x = low;
		double upperbound_x = up;

		low = Double.POSITIVE_INFINITY;
		up = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < ys.length; i++) {
			if (ys[i] < low) {
				low = ys[i];
			}
			if (ys[i] > up) {
				up = ys[i];
			}
		}
		double lowerbound_y = low;
		double upperbound_y = up;
		double width = upperbound_x - lowerbound_x;
		double height = upperbound_y - lowerbound_y;

		return new Rectangle(lowerbound_x, lowerbound_y, width, height);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		npoints = in.readInt();
		xs = new double[npoints];
		ys = new double[npoints];
		for (int i = 0; i < npoints; i++)
			xs[i] = in.readDouble();
		for (int i = 0; i < npoints; i++)
			ys[i] = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(npoints);
		for (int i = 0; i < npoints; i++)
			out.writeDouble(xs[i]);
		for (int i = 0; i < npoints; i++)
			out.writeDouble(ys[i]);
	}

	@Override
	public int getType() {
		return POLYGON;
	}

	@Override
	public Iterator<Point> iterator() {
		return new PolyIterator();
	}

	public class PolyIterator implements Iterator<Point> {
		int count = 0;

		public boolean hasNext() {
			if (count < npoints)
				return true;
			return false;
		}

		public Point next() {
			Point p = new Point(xs[count], ys[count]);
			count++;
			return p;
		}

		public void remove() {
			// not supported.
		}
	}

	@Override
	public void DebugPrint() {
		System.out.println("[POLYGON] " + npoints + " ...");
	}
	
	@Override
	public String toString() {
		StringBuilder s =new StringBuilder();
		s.append("[POLYGON] ");
		for(int i=0;i<npoints;i++){
			s.append(xs[i] + ",");
			s.append(ys[i] + " ");
		}
		return  s.toString();
	}
	
	@Override
	public int size() {
		return 4 + npoints * 8 * 2;
	}

	public int npoints;
	private double[] xs;
	private double[] ys;

	

}