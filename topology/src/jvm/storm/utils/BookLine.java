package storm.utils;

public class BookLine {

	private String line;

	public BookLine(String line){
		this.line = line;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	@Override
	public String toString() {
		return"BookLine: "+line.toString();
	}

}
