package storm.utils;

import java.io.File;
import java.util.ArrayList;

public class Pdf {

	private File file;
	private String name;
	private ArrayList<PdfLine> content;

	public Pdf(String name, File file){
		this.name = name;
		this.file = file;
		this.content = new ArrayList<PdfLine>();
	}

	public void addLine(String lineString){
		content.add(new PdfLine(lineString));
	}

	@Override
	public String toString() {
		String print = "Pdf: " + getName();
		for(PdfLine line : content){
			print += "\n "+line;
		}
		return print;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ArrayList<PdfLine> getContent() {
		return content;
	}

	public void setContent(ArrayList<PdfLine> content) {
		this.content = content;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}


}
