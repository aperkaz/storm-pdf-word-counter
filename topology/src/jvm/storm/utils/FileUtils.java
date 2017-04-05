package storm.utils;

import java.io.File;
import java.io.IOException;
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

	public static ArrayList<String> getPdfContent(File file) {
		String rawPdfContent = null;
		ArrayList<String> pdfContent = new ArrayList<String>();

		PDDocument document = null;
		try {
			document = PDDocument.load(file);
			document.getClass();
			if (!document.isEncrypted()) {
				PDFTextStripperByArea stripper = new PDFTextStripperByArea();
				stripper.setSortByPosition(true);
				PDFTextStripper Tstripper = new PDFTextStripper();
				rawPdfContent = Tstripper.getText(document);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				document.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// populate ArrayList
		String[] lines = rawPdfContent.split(System.getProperty("line.separator"));
		for(int index = 0; index < lines.length; index++){
			pdfContent.add(lines[index]);
		}

		return pdfContent;
	}


}
