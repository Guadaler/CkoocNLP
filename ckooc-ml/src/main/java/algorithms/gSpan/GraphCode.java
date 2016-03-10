package algorithms.gSpan;

import java.util.ArrayList;

/**
 * 图编码类
 * Created by yhao on 2016/3/10.
 */
public class GraphCode {
	//边的集合，边的排序代表着边的添加次序
	ArrayList<Edge> edgeSeq;
	//拥有这些边的图的id
	ArrayList<Integer> gs;
	
	public GraphCode() {
		this.edgeSeq = new ArrayList<>();
		this.gs = new ArrayList<>();
	}

	public ArrayList<Edge> getEdgeSeq() {
		return edgeSeq;
	}

	public void setEdgeSeq(ArrayList<Edge> edgeSeq) {
		this.edgeSeq = edgeSeq;
	}

	public ArrayList<Integer> getGs() {
		return gs;
	}

	public void setGs(ArrayList<Integer> gs) {
		this.gs = gs;
	}
}
