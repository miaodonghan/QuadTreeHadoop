package quadIndex;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class Point extends SpatialObj implements Writable{

	public Point(double x0, double y0){
		x = x0;
		y = y0;
	}
	
	@Override
	public int getType() {
		return POINT;
	}
	
	private boolean intersects(double x0, double y0, double width, double height) {
		if( (x>=x0 && x<=x0+width) &&(y>=y0 && y<=y0+height) )
			return true;
		return false;
	}
	
	@Override
	public boolean intersects(Rectangle rect) {
		return intersects(rect.x, rect.y, rect.width, rect.height);
	}
	/*
	private boolean intersects(double x0, double y0, double radius) {
		double w = x - x0;
		double h = y - y0;
		if(w*w + h*h < radius * radius)
			return true;
		return false;
	}*/
	
	@Override
	public Iterator<Point> iterator() {
		return new PointIterator();
	}
	
	public class PointIterator implements Iterator<Point> {
		boolean firstCall = true;
	    public boolean hasNext() {
	        if(firstCall)
	        	return true;
	        return false;
	    }

	    public Point next() {
	    	firstCall = false;
	        return new Point(x,y);
	    }

	    public void remove() {
	    	// not supported.
	    }
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	@Override
	public void DebugPrint() {
		System.out.println("[POINT] "+ x + " "+ y);
	}
	
	@Override
	public String toString() {
		return "[POINT] "+ x + " "+ y;
	}

	@Override
	public int size() {
		return 8*2;
	}
	
	@Override
	public Rectangle getMBR() {
		return new Rectangle(x,y,0.0,0.0);
	}

	private double x;
	private double y;

}
