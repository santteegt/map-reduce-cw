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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import tl.lin.data.pair.PairOfWritables;

/**
 * Analyze Bigram counts obtained by running the Map Reduce Job BigramCount
 * @author santteegt
 *
 */
public class AnalyzeBigramCount {
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

		List<PairOfWritables<Text, IntWritable>> bigrams;
		try {
			bigrams = readDirectory(new Path(args[0]));

			Collections.sort(bigrams, new Comparator<PairOfWritables<Text, IntWritable>>() {
				public int compare(PairOfWritables<Text, IntWritable> e1,
						PairOfWritables<Text, IntWritable> e2) {
					if (e2.getRightElement().compareTo(e1.getRightElement()) == 0) {
						return e1.getLeftElement().compareTo(e2.getLeftElement());
					}

					return e2.getRightElement().compareTo(e1.getRightElement());
				}
			});

			int twiceOcurrences = 0;
			int sum = 0;
			for (PairOfWritables<Text, IntWritable> bigram : bigrams) {
				sum += bigram.getRightElement().get();

				if (bigram.getRightElement().get() == 2) {
					twiceOcurrences++;
				}
			}

			System.out.println("Number of unique bigrams: " + bigrams.size());
			
			

			System.out.println("\nTOP 20 most frequent bigrams: ");

			int cnt = 0;
			int sumTop20 = 0;
			for (PairOfWritables<Text, IntWritable> bigram : bigrams) {
				System.out.println(bigram.getLeftElement() + "\t" + bigram.getRightElement());
				sumTop20 += bigram.getRightElement().get();
				cnt++;

				if (cnt >= 20) {
					break;
				}
			}
			
			System.out.println("total number of bigrams: " + sum);
			System.out.println("Cumulative frequency of the Top20 bigrams: " + sumTop20);
			System.out.println("Number of bigrams with only two ocurrences: " + twiceOcurrences);
			
		} catch (IOException e) {
			System.err.println("Couldn't load folder: " + args[0]);
		}
	}

	/**
	 *  Reads in the bigram count file 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private static List<PairOfWritables<Text, IntWritable>> readDirectory(Path path) throws IOException {

		File dir = new File(path.toString());
		ArrayList<PairOfWritables<Text, IntWritable>> bigrams = new ArrayList<PairOfWritables<Text, IntWritable>>();

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
			String count;

			while ((rLine = results.readLine()) != null) {
				rToken = new StringTokenizer(rLine);
				firstWord = rToken.nextToken();
				secondWord = rToken.nextToken();
				count = rToken.nextToken();

				bigrams.add(new PairOfWritables<Text, IntWritable>(new Text(firstWord + " "
						+ secondWord), new IntWritable(Integer.parseInt(count))));

			}
			if (bigramFile != null)
				bigramFile.close();
		}

		return bigrams;

	}

}
