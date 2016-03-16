package uk.ac.ucl.irdm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Bean representation of a Bigram
 * @author santteegt
 *
 */
public class Bigram implements WritableComparable<Bigram>{
    private String word;
    private String neighbor;

    public Bigram() {
    }
    
    public Bigram(String neighbor, String word) {
        this.word = word;
        this.neighbor = neighbor;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
    	this.neighbor = in.readUTF();
        this.word = in.readUTF();
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
    	out.writeUTF(this.neighbor);
        out.writeUTF(this.word);
    }
    
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getNeighbor() {
        return neighbor;
    }

    public void setNeighbor(String neighbor) {
        this.neighbor = neighbor;
    }

    public boolean equals(Object obj){
        Bigram pair = (Bigram)obj;
        return (this.word.equals(pair.getWord())) && (this.neighbor.equals(pair.getNeighbor()));
    }
    
    @Override
    public int compareTo(Bigram pair) {
        String pl = pair.getNeighbor();
        String pr = pair.getWord();
    
        if (this.neighbor.equals(pl)) {
            return this.word.compareTo(pr);
        }
    
        return this.neighbor.compareTo(pl);
    }
    
    @Override
    public int hashCode() {
    	return this.neighbor.hashCode() + this.word.hashCode();
    }
    
    @Override
    public String toString(){
      return "(" + this.neighbor + ", " + this.word + ")";
    }
    
    /**
     * Clones this object.
     *
     * @return clone of this object
     */
    @Override
    public Bigram clone() {
      return new Bigram(this.neighbor, this.word);
    }

}
