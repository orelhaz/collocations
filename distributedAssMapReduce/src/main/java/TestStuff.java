public class TestStuff {

	public static void main(String[] args) {
		
		double res = likelihoodRatioFromInt(1, 284, 1, 2658);
		
		System.out.println(res);
	}
	
	public static double likelihoodRatioFromInt (int c1, int c2, int c12, int N)
	{
		return likelihoodRatio(c1, c2, c12, N);
	}
	
	public static double likelihoodRatio(double c1, double c2, double c12, int N)
    {
        double p = c2/N;
        double p1 = c12/N;
        double p2 = (c2-c12)/(N-c1);

        double L1 = L (c12, c1, p);
        double L2 = L (c2-c12, N-c1, p);
        double L3 = L (c12, c1, p1);
        double L4 = L (c2 - c12, N - c1, p2);
        
        double logM1 = L1 == 0 ? 0 : Math.log(L1);
        double logM2 = L2 == 0 ? 0 : Math.log(L2);
        double logM3 = L3 == 0 ? 0 : Math.log(L3);
        double logM4 = L4 == 0 ? 0 : Math.log(L4);
        
        return logM1 + logM2 - logM3 - logM4;

    }
	
	public static double L (double k, double n, double x)
	{
		return Math.pow(x, k) * Math.pow(1-x, n-k);
	}

}
