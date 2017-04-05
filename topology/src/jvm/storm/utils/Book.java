package storm.utils;

import java.io.File;
import java.util.ArrayList;

public class Book {

	private File file;
	private String name;
	private ArrayList<BookLine> content;

	public Book(String name, File file){
		this.name = name;
		this.file = file;
		this.content = new ArrayList<BookLine>();
	}

	public void addLine(String lineString){
		content.add(new BookLine(lineString));
	}

	@Override
	public String toString() {
		String print = "Book: " + getName();
		for(BookLine line : content){
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

	public ArrayList<BookLine> getContent() {
		return content;
	}

	public void setContent(ArrayList<BookLine> content) {
		this.content = content;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}


}
