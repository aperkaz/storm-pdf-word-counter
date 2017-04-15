package storm.utils;

public class PdfLine {

	private String line;

	public PdfLine(String line){
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
		return"PdfLine: "+line.toString();
	}

}
