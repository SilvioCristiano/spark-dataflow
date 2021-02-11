package com.ocidf.spark.bda.common;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import java.util.*;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.*;
import java.io.File;
import java.io.IOException;

public class ReadPdf 
{
	public static void main(String args[]) throws IOException 
	{
		try
		{
			PDDocument document = PDDocument.load(new File("data/NFSe_00000054_10696038.pdf"));// here file1.pdf is the name of pdf file which we want to read....
			document.getClass();

			
			if (!document.isEncrypted())
			{
				PDFTextStripperByArea stripper = new PDFTextStripperByArea();
				stripper.setSortByPosition(true);
				PDFTextStripper Tstripper = new PDFTextStripper();
				String str = Tstripper.getText(document);
				Scanner scn = null;					
				scn = new Scanner(str);
				String line="";
				while (scn.hasNextLine()) 
				{		
					line = scn.nextLine();
					System.out.println("\n"+line);
				}	
			}
			document.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
			
	}
}