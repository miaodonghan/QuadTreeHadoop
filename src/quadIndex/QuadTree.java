package quadIndex;
/*
 * QuadTree Index Structure for MapReduce
 * Author: Donghan Miao
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Writable;

public class QuadTree implements Writable {

	public QuadTree(int x, int y, int width, int height) {		
		root = new QuadInternalNode(x, y, width, height);
	}

	public QuadTree() {
		root = new QuadInternalNode(0, 0, 1000000, 1000000);
	}

	/*
	 * @deprecated
	 */

	public void insert(SpatialObj obj, int offset) {
		root.insert(obj,offset);
	}

	public Set<Integer> RangeQuery(Rectangle rect) {
		return root.RangeQuery(rect);
	}

	public Set<FileLoc> cur_RangeQuery(Rectangle rect) {
		return root.cur_RangeQuery(rect);
	}

	public int Count() {
		return root.Count();
	}

	public void DebugOutput() {
		root.DebugOutput();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		in.readInt();// size of the whole tree.
		Rectangle rect = new Rectangle(0,0,0,0);
		rect.readFields(in);
		root = new QuadInternalNode(rect);
		root.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(root.size());
		root.bound.write(out);
		root.write(out);
	}

	public int size() {
		return root.size();
	}

	QuadInternalNode root;
}

abstract class QuadNode implements Writable {
	@Deprecated
	public abstract Set<Integer> RangeQuery(Rectangle rect);

	public abstract Set<FileLoc> cur_RangeQuery(Rectangle rect);

	public abstract int Count();

	public abstract boolean isLeaf();

	public abstract void DebugOutput();

	public abstract int size();

	protected Rectangle bound;
}

class QuadInternalNode extends QuadNode {
	public QuadInternalNode(int x, int y, int width, int height) {
		bound = new Rectangle(x, y, width, height);
		children = new QuadNode[4];
		int mid_x = x + width / 2;
		int mid_y = y + height / 2;
		children[0] = new QuadLeafNode(x, y, mid_x - x, mid_y - y, null);
		children[1] = new QuadLeafNode(mid_x + 1, y, x + width - mid_x, mid_y
				- y, null);
		children[2] = new QuadLeafNode(x, mid_y + 1, mid_x - x, y + height
				- mid_y, null);
		children[3] = new QuadLeafNode(mid_x + 1, mid_y + 1, x + width - mid_x,
				y + height - mid_y, null);
	}
	public QuadInternalNode(Rectangle rect){
		this((int)rect.x, (int)rect.y, (int)rect.width, (int)rect.height);
	}
	
	public QuadInternalNode(QuadLeafNode ln) {
		bound = ln.bound;
		Rectangle rect = ln.bound;
		children = new QuadNode[4];
		double mid_x = rect.x + rect.width / 2;
		double mid_y = rect.y + rect.height / 2;
		children[0] = new QuadLeafNode(rect.x, rect.y, mid_x - rect.x, mid_y
				- rect.y, null);
		children[1] = new QuadLeafNode(mid_x + 1, rect.y, rect.x + rect.width
				- mid_x, mid_y - rect.y, null);
		children[2] = new QuadLeafNode(rect.x, mid_y + 1, mid_x - rect.x,
				rect.y + rect.height - mid_y, null);
		children[3] = new QuadLeafNode(mid_x + 1, mid_y + 1, rect.x
				+ rect.width - mid_x, rect.y + rect.height - mid_y, null);

		ArrayList<SpatialObj> oList = ln.getObjList();
		if(oList == null)
			return;
		for (int i = 0; i < 4; i++) {
			for (SpatialObj o : oList) {
				if (o.intersects(children[i].bound)) {
					((QuadLeafNode) children[i]).setObjList(new ArrayList<SpatialObj>(oList));
					((QuadLeafNode) children[i]).setLocList(new ArrayList<FileLoc>(ln.getLocList()));
					((QuadLeafNode) children[i]).num_of_obj = ln.num_of_obj;
					break;
				}
			}
		}
	}

	// this is used in de-serialization, from byte stream.
	// The boundary range is automatically computed by its parent.
	public QuadInternalNode(Rectangle pRect, int section) {
		Rectangle rect = null;
		double mid_x = pRect.x + pRect.width / 2;
		double mid_y = pRect.y + pRect.height / 2;
		switch (section) {
		case 0:
			rect = new Rectangle(pRect.x, pRect.y, mid_x - pRect.x, mid_y
					- pRect.y);
			break;
		case 1:
			rect = new Rectangle(mid_x + 1, pRect.y, pRect.x + pRect.width
					- mid_x, mid_y - pRect.y);
			break;
		case 2:
			rect = new Rectangle(pRect.x, mid_y + 1, mid_x - pRect.x,
					pRect.y + pRect.height - mid_y);
			break;
		case 3:
			rect = new Rectangle(mid_x + 1, mid_y + 1, pRect.x + pRect.width
					- mid_x, pRect.y + pRect.height - mid_y);
			break;
		}
		bound = rect;
		children = new QuadNode[4];
	}

	public void insert(SpatialObj _obj, int offset) {
		for (int i = 0; i < 4; i++) {
			if (!_obj.intersects(children[i].bound))
				continue;
			if (children[i].isLeaf()) {
				QuadLeafNode ln = (QuadLeafNode) children[i];
				if (ln.isNull()) {
					ln.addObj(_obj);
					ln.addFileLoc(new FileLoc(offset, _obj.size()));
					ln.num_of_obj++;
				} else {

					ArrayList<SpatialObj> content = ln.getObjList();
					for (SpatialObj o : content) {
						if (o.intersects(_obj.getMBR())) {
							ln.addObj(_obj);
							ln.addFileLoc(new FileLoc(offset, _obj.size()));
							ln.num_of_obj++;
							return;
						}
					}
					children[i] = new QuadInternalNode(ln);
					QuadInternalNode node = (QuadInternalNode) children[i];
					node.insert(_obj,offset);
				}
			} else {
				((QuadInternalNode) children[i]).insert(_obj,offset);
			}
		}
	}

	@Deprecated
	@Override
	public Set<Integer> RangeQuery(Rectangle rect) {
		Set<Integer> results = new HashSet<Integer>();
		for (int i = 0; i < 4; i++) {
			if (rect.intersects(children[i].bound)) {
				Set<Integer> r = children[i].RangeQuery(rect);
				if (r!= null && r.size()>0)
					results.addAll(r);
			}
		}
		return results;
	}

	@Override
	public Set<FileLoc> cur_RangeQuery(Rectangle rect) {
		Set<FileLoc> results = new HashSet<FileLoc>();
		for (int i = 0; i < 4; i++) {
			if (rect.intersects(children[i].bound)) {
				Set<FileLoc> r = children[i].cur_RangeQuery(rect);
				if (r!=null && r.size()>0)
					results.addAll(r);
			}
		}
		return results;
	}

	@Override
	public boolean isLeaf() {
		return false;
	}

	public int Count() {
		int count = 0;
		for (int i = 0; i < 4; i++) {
			count += children[i].Count();
		}
		return count;
	}

	@Override
	public void DebugOutput() {
		for (int i = 0; i < 4; i++) {
			children[i].DebugOutput();
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		for (int i = 0; i < 4; i++) {
			boolean isLeaf = in.readBoolean();
			if (isLeaf) {
				children[i] = new QuadLeafNode(bound, i);
			} else {
				children[i] = new QuadInternalNode(bound, i);
			}
			children[i].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (int i = 0; i < 4; i++) {
			if (children[i].isLeaf()) {
				out.writeBoolean(true);
			} else {
				out.writeBoolean(false);
			}
			children[i].write(out);
		}
	}

	@Override
	public int size() {
		int _size = 0;
		for (int i = 0; i < 4; i++) {
			_size += children[i].size();
		}
		return _size;
	}

	protected QuadNode[] children;
}

class QuadLeafNode extends QuadNode {
	public QuadLeafNode(double x, double y, double width, double height,
			SpatialObj o) {
		bound = new Rectangle(x, y, width, height);
		objList = new ArrayList<SpatialObj>();
		locList = new ArrayList<FileLoc>();
		if (o != null)
			objList.add(o);

	}

	public void setObjList(ArrayList<SpatialObj> olist) {
		objList = olist;
	}
	
	public void setLocList(ArrayList<FileLoc> lList){
		locList = lList;
	}

	public QuadLeafNode(Rectangle pRect, int section) {
		Rectangle m_bound = null;
		double mid_x = pRect.x + pRect.width / 2;
		double mid_y = pRect.y + pRect.height / 2;
		switch (section) {
		case 0:
			m_bound = new Rectangle(pRect.x, pRect.y, mid_x - pRect.x, mid_y
					- pRect.y);
			break;
		case 1:
			m_bound = new Rectangle(mid_x + 1, pRect.y, pRect.x + pRect.width
					- mid_x, mid_y - pRect.y);
			break;
		case 2:
			m_bound = new Rectangle(pRect.x, mid_y + 1, mid_x - pRect.x,
					pRect.y + pRect.height - mid_y);
			break;
		case 3:
			m_bound = new Rectangle(mid_x + 1, mid_y + 1, pRect.x + pRect.width
					- mid_x, pRect.y + pRect.height - mid_y);
			break;
		}
		bound = m_bound;
		objList = new ArrayList<SpatialObj>();
		locList = new ArrayList<FileLoc>();
	}

	public boolean isNull() {
		return (num_of_obj == 0);
	}

	@Override
	public boolean isLeaf() {
		return true;
	}

	/*
	 * @Deprecated
	 * 
	 * @Override public Set<Integer> RangeQuery(Rectangle rect) { if (obj ==
	 * null) return null; if (obj.intersects(rect)) { Set<Integer> me = new
	 * HashSet<Integer>(); me.add(obj.getId()); return me; } else { return null;
	 * } }
	 */
	public ArrayList<SpatialObj> getObjList() {
		return objList;
	}

	public ArrayList<FileLoc> getLocList() {
		return locList;
	}
	
	public void addObj(SpatialObj o) {
		objList.add(o);
	}

	public void addFileLoc(FileLoc e) {
		locList.add(e);
	}

	@Override
	public Set<FileLoc> cur_RangeQuery(Rectangle rect) {
		if (isNull())
			return null;
		Set<FileLoc> me = new HashSet<FileLoc>();
		me.addAll(locList);
		/*for (int i = 0; i < num_of_obj; i++) {
			if (objList.get(i).intersects(rect)) {
				me.add(locList.get(i));
			}
		}*/
		return me;
	}

	@Override
	public int Count() {
		return this.num_of_obj;
	}

	@Override
	public void DebugOutput() {
		// if (!isNull())
		// obj.DebugPrint();
	}

	@Override
	public int size() {
		return num_of_obj * FileLoc.size();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		num_of_obj = in.readInt();
		for (int i = 0; i < num_of_obj; i++) {
			int pos = in.readInt();
			int len = in.readInt();
			locList.add(new FileLoc(pos,len));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(num_of_obj);
		for (int i = 0; i < num_of_obj; i++) {
			out.writeInt(locList.get(i).offset);
			out.writeInt(locList.get(i).length);
		}
	}

	@Override
	public Set<Integer> RangeQuery(Rectangle rect) {
		// TODO Auto-generated method stub
		return null;
	}

	// ultimately this is expendable.
	int num_of_obj = 0;
	private ArrayList<SpatialObj> objList;
	private ArrayList<FileLoc> locList;

}
