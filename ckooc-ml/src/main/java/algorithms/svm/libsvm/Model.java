package algorithms.svm.libsvm;

/**
 * SVM模型类
 * Created by yhao on 2016/3/10.
 */
public class Model implements java.io.Serializable {
	//svm支持向量机的参数
	Parameter param;	// parameter
	//分类的类型数
	int nr_class;		// number of classes, = 2 in regression/one class SVM
	int l;			// total #SV
	Node[][] SV;	// SVs (SV[l])
	double[][] sv_coef;	// coefficients for SVs in decision functions (sv_coef[k-1][l])
	double[] rho;		// constants in decision functions (rho[k*(k-1)/2])
	double[] probA;         // pariwise probability information
	double[] probB;

	// for classification only

	//每个类型的类型值
	int[] label;		// label of each class (label[k])
	int[] nSV;		// number of SVs for each class (nSV[k])
	// nSV[0] + nSV[1] + ... + nSV[k-1] = l
}
