package uk.ac.bbk.ucl.cw.util;

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
    
    public Bigram(String word, String neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        this.word = in.readUTF();
        this.neighbor = in.readUTF();
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeUTF(this.word);
        out.writeUTF(this.neighbor);
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
        String pl = pair.getWord();
        String pr = pair.getNeighbor();
    
        if (this.word.equals(pl)) {
            return this.neighbor.compareTo(pr);
        }
    
        return this.word.compareTo(pl);
    }
    
    public String toString(){
      return "(" + this.neighbor + ", " + this.word + ")";
    }  
}
