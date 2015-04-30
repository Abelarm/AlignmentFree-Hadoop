package hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class DistanceGenericWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
        	DoubleWritable.class,
        	DistanceDoubleWritable.class,
        	DistanceCOWritable.class
        };
    }
    //this empty initialize is required by Hadoop
    public DistanceGenericWritable() {
    }

    public DistanceGenericWritable(Writable instance) {
        set(instance);
    }
    
    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
    
  
    @Override
    public String toString() {
        return "DistanceGenericWritable [" + this.get().toString() + "]";
    }
}
