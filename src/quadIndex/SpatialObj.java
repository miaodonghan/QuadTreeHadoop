package quadIndex;


import org.apache.hadoop.io.Writable;

public abstract class SpatialObj implements Iterable<Point>, Writable{ // 
	public static final int POINT = 1;
	public static final int RECT = 2;
	public static final int POLYGON = 3;
	
	public abstract int getType();
	//public abstract boolean intersects(float x, float y, float width, float height);
	public abstract boolean intersects(Rectangle rect);
	/*public abstract boolean intersects(float x, float y, float radius);*/
	public abstract int size();
	public abstract void DebugPrint();
	public abstract Rectangle getMBR();
	public abstract String toString();
}
