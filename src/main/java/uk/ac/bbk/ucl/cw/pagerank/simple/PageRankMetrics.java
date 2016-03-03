package uk.ac.bbk.ucl.cw.pagerank.simple;

/**
 * 
 * @author santteegt
 *
 */
public class PageRankMetrics {
	
	  protected static float sumLogProbs(float a, float b) {
	    if (a == Float.NEGATIVE_INFINITY)
	      return b;

	    if (b == Float.NEGATIVE_INFINITY)
	      return a;

	    if (a < b) {
	      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
	    }

	    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
	  }

}
