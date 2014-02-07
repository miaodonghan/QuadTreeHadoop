package quadIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/*
 * A helper Class as a substitude for java.awt.Rectangle
 * Author: Li Wang, Donghan Miao
 */
public class Rectangle extends SpatialObj implements Writable {

	public float getWidth(){
		return width;
	}
	
	public Rectangle(float _x, float _y, float w, float h) {
		x = _x;
		y = _y;
		width = w;
		height = h;
	}

	public boolean isEmpty() {
		if (width == 0 || height == 0) {
			return true;
		}
		return false;
	}

	public boolean contains(Rectangle rect) {
		// check the down-left point
		if (x <= rect.x && y <= rect.y) {
			// check the up-right point
			if (x + width >= rect.x + rect.width
					&& y + height >= rect.y + rect.height) {
				return true;
			}
		}
		return false;
	}

	// check if two lines are overlaped
	private boolean isOverlap(float x1, float x2, float _x1, float _x2) {
		if (x2 < _x1 || x1 > _x2) {
			return false;
		}
		return true;
	}

	@Override
	public boolean intersects(Rectangle rect) {
		if (isOverlap(x, x + width, rect.x, rect.x + rect.width)
				&& isOverlap(y, y + height, rect.y, rect.y + rect.height)) {
			return true;
		}
		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readFloat();
		y = in.readFloat();
		width = in.readFloat();
		height = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(x);
		out.writeFloat(y);
		out.writeFloat(width);
		out.writeFloat(height);
	}

	@Override
	public int getType() {
		return RECT;
	}
	
/*
	private boolean intersects(float x0,float y0,float w,float h) {
		if (!(x > x0 + w || x + width < x0) && !(y > y0 + h || y + height < y0))
			return true;
		return false;
	}

	@Override
	public boolean intersects(float x0, float y0, float radius) {
		if (Math.abs(x - x0) < radius || Math.abs(x + width - x0) < radius
				|| Math.abs(y - y0) < radius
				|| Math.abs(y + height - y0) < radius)
			return true;
		return false;
	}*/

	public Iterator<Point> iterator() {
		return new RectIterator();
	}

	public class RectIterator implements Iterator<Point> {
		int count = 0;

		public boolean hasNext() {
			if (count < 4)
				return true;
			return false;
		}

		public Point next() {
			count++;
			return new Point(x + count / 2 * width, y + count % 2 * height);
		}

		public void remove() {
			// not supported.
		}
	}

	@Override
	public void DebugPrint() {
		System.out
				.println("[RECT] " + x + " " + y + " " + width + " " + height);

	}

	@Override
	public int size() {
		return 4*4;
	}
	
	@Override
	public Rectangle getMBR() {
		return this;
	}	
	
	@Override
	public String toString() {
		return "[RECT] " + x + " " + y + " " + width + " " + height;
	}

	public float x;
	public float y;
	public float width;
	public float height;

}