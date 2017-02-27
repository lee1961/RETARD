package heap;
import java.util.Arrays;

public class Tuple{
	int offset;
	int length;
	byte[] data;

	public Tuple() {}

	public Tuple(byte[] data, int offset, int length){
		this.offset = offset;
		this.length = length;
		this.data = Arrays.copyOf(data, data.length);
	}

	public int getLength(){
		return length;
	}

	public byte[] getTupleByteArray(){
		return this.data;
	}
}

