package com.ocidf.spark.bda.common;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;

public class PdfParaImagem {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		File input = new File("src\\Paginas\\ArquivoComConteudo.pdf");
		PDDocument doc = PDDocument.load(new FileInputStream(input));

		List<PDPage> pages = doc.getDocumentCatalog().getAllPages();
		Iterator<PDPage> i = pages.iterator();
		int count = 1;
		while (i.hasNext()) {
			PDPage page = i.next();
			BufferedImage bi = page.convertToImage();
			ImageIO.write(bi, "jpg", new File("src\\Paginas\\pdfimage" + count + ".jpg"));
			count++;
		}
		System.out.println("Conversão Completa!");
	}
}
