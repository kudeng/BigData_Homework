import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements Writable {

    private int up;
    private int down;
    private int total;

    public Flow() {
    }

    public Flow(int up, int down){
        this.up = up;
        this.down = down;
        this.total = up + down;
    }

    public Flow(int up, int down, int total){
        this.up = up;
        this.down = down;
        this.total = total;
    }

    public int getUp() {
        return up;
    }

    public int getDown() {
        return down;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(up);
        out.writeInt(down);
        out.writeInt(total);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        up = in.readInt();
        down = in.readInt();
        total = in.readInt();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Integer.hashCode(up);
        result = prime * result + (int)(down ^ (down >>> 32));
        return result;
    }

//    @Override
//    public int compareTo(Flow o) {
//        return Integer.compare(this.up + this.down, o.up + o.down);
//    }
}