package hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class KmerGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
        	KmerCountWritable.class,
        	KmerCOWritable.class,
        	Text.class,
        	ArrayKmerCountWritable.class,
        	ArrayKmerCOWritable.class
        };
    }
    //this empty initialize is required by Hadoop
    public KmerGenericWritable() {
    }

    public KmerGenericWritable(Writable instance) {
        set(instance);
    }
    
    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
    
  
    @Override
    public String toString() {
        return "MyGenericWritable [" + this.get().toString() + "]";
    }
}