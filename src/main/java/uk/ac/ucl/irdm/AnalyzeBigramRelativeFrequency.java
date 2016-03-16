package uk.ac.ucl.irdm;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfWritables;

/**
 * Analyze Bigram relative frequencies obtained by running the Map Reduce Job BigramRelativeFrequency
 * @author santteegt
 *
 */
public class AnalyzeBigramRelativeFrequency {
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

		List<PairOfWritables<PairOfStrings, FloatWritable>> pairs;
		try {
			pairs = readDirectory(new Path(args[0]));

			List<PairOfWritables<PairOfStrings, FloatWritable>> list1 = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();

			double probSentence = Math.log(0.0032);
			for (PairOfWritables<PairOfStrings, FloatWritable> p : pairs) {
				PairOfStrings bigram = p.getLeftElement();

				if (bigram.getLeftElement().equals("romeo")) {
					list1.add(p);
				}
				//P(romeo is the king) with P(romeo)=0.0032
				if(bigram.getLeftElement().equals("romeo") && bigram.getRightElement().equals("is")) {
					probSentence += Math.log(p.getValue().get());
					System.out.println("P(is|romeo)="+ p.getValue().get());
				}
				if(bigram.getLeftElement().equals("is") && bigram.getRightElement().equals("the")) {
					probSentence += Math.log(p.getValue().get());
					System.out.println("P(the|is)="+ p.getValue().get());
				}
				if(bigram.getLeftElement().equals("the") && bigram.getRightElement().equals("king")) {
					probSentence += Math.log(p.getValue().get());
					System.out.println("P(king|the)="+ p.getValue().get());
				}
				
			}

			Collections.sort(list1,
					new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
						public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
								PairOfWritables<PairOfStrings, FloatWritable> e2) {
							if (e1.getRightElement().compareTo(e2.getRightElement()) == 0) {
								return e1.getLeftElement().compareTo(e2.getLeftElement());
							}

							return e2.getRightElement().compareTo(e1.getRightElement());
						}
					});

			System.out.println("\nTop 5 most frequent words given 'romeo'");
			int i = 0;
			for (PairOfWritables<PairOfStrings, FloatWritable> p : list1) {
				PairOfStrings bigram = p.getLeftElement();
				System.out.println(bigram + "\t" + p.getRightElement());
				i++;

				if (i > 5) {
					break;
				}
			}
			
			System.out.println(String.format("\nGiven P(romeo)=0.0032 -> P(romeo is the king) = %.10f", Math.exp(probSentence) ));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Reads in the bigram relative frequency count file
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private static List<PairOfWritables<PairOfStrings, FloatWritable>> readDirectory(Path path)
			throws IOException {

		File dir = new File(path.toString());
		ArrayList<PairOfWritables<PairOfStrings, FloatWritable>> relativeFrequencies = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();
		for (File child : dir.listFiles()) {
			if (".".equals(child.getName()) || "..".equals(child.getName()) || child.getName().startsWith(".")) {
				continue;
			}
			FileInputStream bigramFile = null;

			bigramFile = new FileInputStream(child.toString());

			DataInputStream resultsStream = new DataInputStream(bigramFile);
			BufferedReader results = new BufferedReader(new InputStreamReader(resultsStream));

			StringTokenizer rToken;
			String rLine;
			String firstWord;
			String secondWord;
			String frequency;

			while ((rLine = results.readLine()) != null) {
				rToken = new StringTokenizer(rLine);
				firstWord = rToken.nextToken();
				firstWord = firstWord.substring(1, firstWord.length() - 1);
				secondWord = rToken.nextToken();
				secondWord = secondWord.substring(0, secondWord.length() - 1);
				frequency = rToken.nextToken();

				relativeFrequencies.add(new PairOfWritables<PairOfStrings, FloatWritable>(
						new PairOfStrings(firstWord, secondWord), new FloatWritable(Float
								.parseFloat(frequency))));

			}
			if (bigramFile != null)
				bigramFile.close();
		}

		return relativeFrequencies;

	}
}
