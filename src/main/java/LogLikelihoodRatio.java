import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import org.apache.commons.math3.distribution.BinomialDistribution;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
//import org.apache.mahout.math.stats.LogLikelihood;

/**
 * Utility methods for working with log-likelihood
 */
public final class LogLikelihoodRatio {

    private LogLikelihoodRatio() {
    }

    /**
     *
     * @param c1 -
     * @param c2 -
     * @param c12 -
     * @param N - total number of ngrams in decade
     * @return
     */
    public static double likelihoodRatio(int c1, int c2, int c12, int N)
    {
        double p = c2/N;
        double p1 = c12/N;
        double p2 = (c2-c12)/(N-c1);

        double b1 = new BinomialDistribution(c12,p).cumulativeProbability(c1);
        double b2 = new BinomialDistribution(c2 - c12,p).cumulativeProbability(N - c1);
        double b3 = new BinomialDistribution(c12,p1).cumulativeProbability(c1);
        double b4 = new BinomialDistribution(c2 - c12,p2).cumulativeProbability(N - c1);

        return Math.log(b1) + Math.log(b2) - Math.log(b3) - Math.log(b4);
    }
}


