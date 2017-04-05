package storm.utils;

import java.io.File;
import java.util.ArrayList;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;

public class FileUtils {

	final static String PDF_EXTENSION = "pdf";

	/**
	 * Generate PDF file list from directory
	 * @param directory
	 * @return
	 */
	public static ArrayList<File> extractPdfFiles(String directory){
		ArrayList<File> pdfFiles = new ArrayList<File>();

		File files[] = new File(directory).listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isFile()) {
					String[] type = file.getName().toString().split("\\.(?=[^\\.]+$)");
					if (type.length > 1 && type[1].equals(PDF_EXTENSION)) {
						pdfFiles.add(file);
					}
				}
			}
		}
		return pdfFiles;
	}

	public static ArrayList<Book> populateBookContent(ArrayList<Book> books){
		ArrayList<Book> populatedBooks = books;

		for(Book book : populatedBooks){

			String pdfContent = extractPdfContext(book.getFile());
		    // populate book
		    String[] lines = pdfContent.split(System.getProperty("line.separator"));
		    for(int index = 0; index < lines.length; index++){
		    	book.addLine(lines[index]);
		    }

		}

		return populatedBooks;
	}

	private static String extractPdfContext(File file){
		String pdfContent = null;
		try{
			PDDocument document = null;
			document = PDDocument.load(file);
			document.getClass();
			if( !document.isEncrypted() ){
			    PDFTextStripperByArea stripper = new PDFTextStripperByArea();
			    stripper.setSortByPosition( true );
			    PDFTextStripper Tstripper = new PDFTextStripper();
			    pdfContent = Tstripper.getText(document);
			}
		}catch(Exception e){
		    e.printStackTrace();
		}
		return pdfContent;
	}



	public static void printFileList(ArrayList<File> files){
		for(File file : files){
			System.out.println(""+file.getName());
		}
	}

}
