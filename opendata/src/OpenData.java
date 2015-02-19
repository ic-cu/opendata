import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import sql.DB;

public class OpenData
{
	private DB db;
	private Properties config, dbconfig, qconfig;
	private String today;
	private String tempDir;
	private String csvFS, csvTS, csvBOM;
	private Logger log;
	private long totalStart, totalStop, partialStart, partialStop;
	private SimpleDateFormat dateStampFormat;
	private String labelIsil;

	private String trim(String field)
	{
		if(field != null)
		{
			field = field.replaceAll("\t+", " ");
			field = field.replaceAll("\n+", " ");
			field = field.replaceAll(" +", " ");
			return field;
		}
		else
			return "";
	}

	private String wrap(String field, boolean last)
	{
		String tmp = csvTS + field + csvTS;
		if(!last)
		{
			tmp += csvFS;
		}
		return tmp;
	}

	private String wrap(String field)
	{
		return wrap(field, false);
	}

	private void initLogger() throws FileNotFoundException
	{
		// logger generico
		PatternLayout pl;
		File lf;
		PrintWriter pw;
		WriterAppender wa;
		log = Logger.getLogger("OPENDATA");
		log.setLevel(Level.INFO);
		pl = new PatternLayout(config.getProperty("log.pattern"));
		lf = new File(config.getProperty("log.file"));
		pw = new PrintWriter(lf);
		wa = new WriterAppender(pl, pw);
		log.addAppender(wa);
		wa = new WriterAppender(pl, System.out);
		log.addAppender(wa);
	}

	public OpenData()
	{
		config = new Properties();
		dbconfig = new Properties();
		qconfig = new Properties();
		try
		{
			config.load(new FileReader("opendata.prop"));
			dbconfig.load(new FileReader("db.prop"));
			qconfig.load(new FileReader("query.prop"));
			initLogger();
		}
		catch(FileNotFoundException e)
		{
			log.warn("File non trovato: " + e.getMessage());
		}
		catch(IOException e)
		{
			log.error("Impossibile leggere il file di configurazione: "
					+ e.getMessage());
		}
		String url = dbconfig.getProperty("db.url");
		String user = dbconfig.getProperty("db.user");
		String pass = dbconfig.getProperty("db.pass");
		db = new DB(DB.mysqlDriver, url, user, pass);

		SimpleDateFormat sdf;
		sdf = new SimpleDateFormat("yyyyMMdd");
		today = sdf.format(new Date());

		dateStampFormat = new SimpleDateFormat(
				config.getProperty("dateStamp.pattern"));

		tempDir = config.getProperty("temp.dir");

		if(config.getProperty("temp.dir.daily") != null)
		{
			tempDir += "/" + today;
		}
		File tDir = null;
		tDir = new File(tempDir);
		tDir.mkdirs();

		csvFS = config.getProperty("csv.fs");
		csvTS = config.getProperty("csv.ts");
		csvBOM = config.getProperty("csv.bom");
		labelIsil = config.getProperty("label.xml.isil");
		log.info("Separatore campi per formato CSV [" + csvFS + "]");
		log.info("Separatore testo per formato CSV [" + csvTS + "]");
	}

	public String territorio()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		stmt = db.prepare(qconfig.getProperty("territorio.query"));
		bibs = db.select(qconfig.getProperty("censite.query"));
		String isil, denominazione, fonte, urlFonte;
		int idBib;
		String query = qconfig.getProperty("territorio.query");
		log.debug("Query: " + query);
		ResultSetMetaData rsmd;
		StringWriter output = new StringWriter();
		PrintWriter pw = null;
		String row = null, cell, contatto, note;
		String tel = "", fax = "", mail = "", url = "";
		String oldIsil = "";
		int tipo;
		int limit = Integer.MAX_VALUE;
		int columns = 0;
		int i;
		log.info("Elaborazione territorio");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
		}
		catch(NumberFormatException e)
		{
			log.warn("Massimo numero di biblioteche da elaborare ignorato, si userà il massimo intero possibile");
		}
		partialStart = System.nanoTime();
		try
		{
			boolean headerOk = false;
			while(bibs.next() && limit > 0)
			{
				limit--;
				isil = bibs.getString("isil");
				idBib = bibs.getInt("id");
				denominazione = bibs.getString("denominazione");
				fonte = bibs.getString("fonte");
				urlFonte = bibs.getString("url-fonte");
				pw = new PrintWriter(output);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				while(bib.next() && limit-- > 0)
				{
					try
					{

// una sola volta si crea l'header

						if(!headerOk)
						{
							rsmd = bib.getMetaData();
							columns = rsmd.getColumnCount() - 3;
							String header = csvBOM;
							row = "";
							cell = "";
							header += wrap(labelIsil);
							header += wrap("denominazione");
							for(i = 1; i < columns; i++)
							{
								header += wrap(rsmd.getColumnLabel(i));
							}
							header += wrap(rsmd.getColumnLabel(i));

// si aggiungono all'header quattro campi che saranno riempiti in
// base ai tipi di contatti rinvenuti

							header += wrap("telefono");
							header += wrap("fax");
							header += wrap("email");
							header += wrap("url");
							header += wrap("fonte");
							header += wrap("url-fonte", true);
							pw.println(header);
							headerOk = true;
						}

						if(!isil.equals(oldIsil))
						{
							if(oldIsil != "")
							{
								row += wrap(tel) + wrap(fax) + wrap(mail) + wrap(url)
										+ wrap(trim(fonte)) + wrap(urlFonte, true);
								pw.println(row);
								pw.flush();
							}
							row = "";
							row += wrap(isil) + wrap(denominazione);
							for(i = 1; i < columns; i++)
							{
								cell = bib.getString(i);
								if(cell == null)
								{
									cell = "";
								}
								row += wrap(cell.trim());
							}
							row += wrap(bib.getString(i));
							oldIsil = isil;
							tel = fax = mail = url = fonte = urlFonte = "";
						}

						// vanno gestiti i possibili contatti
						contatto = bib.getString("contatto");

						note = bib.getString("note");
						tipo = bib.getInt("tipo");
						if(contatto != null)
						{
							contatto = contatto.trim();
							if(note == null || note.trim() == "")
							{
								/*
								 * i contatti vanno selezionati per codice, perché il right join
								 * non funziona se si estraggono anche i codici e le descrizioni
								 */
								switch(tipo)
								{
									case 1:
										// telefono
										if(tel == "") tel = contatto;
										break;
									case 2:
										// fax
										if(fax == "") fax = contatto;
										break;
									case 3:
										// mail
										if(mail == "") mail = contatto;
										break;
									case 5:
										// url
										if(url == "") url = contatto;
										break;
									default:
										break;
								}
							}
						}
					}
					catch(SQLException e)
					{
						log.error("Errore SQL: " + e.getMessage());
					}
				}
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		pw.close();
		partialStop = System.nanoTime();
		log.info("Elaborazione territorio terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return output.getBuffer().toString();
	}

	public Document patrimonio()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		stmt = db.prepare(qconfig.getProperty("patrimonio.query"));
		bibs = db.select(qconfig.getProperty("censite.query"));
		String isil, denominazione, nome, categoria;
		int totalePosseduto, acquistiUltimoAnno = 0;
		int idBib;
		Document doc = new Document();
		Element root = new Element("biblioteche");
		root.setAttribute("data-export", dateStampFormat.format(new Date())
				.replaceFirst("[0-9][0-9]$", ""));
		Element biblioteca;
		Element patrimonioElement;
		doc.setRootElement(root);
		int limit = Integer.MAX_VALUE;
		log.info("Elaborazione patrimonio");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
		}
		catch(NumberFormatException e)
		{
			log.warn("Massimo numero di biblioteche da elaborare ignorato, si userà il massimo intero possibile");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				isil = bibs.getString("isil");
				idBib = bibs.getInt("id");
				denominazione = bibs.getString("denominazione");
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute(labelIsil, isil);
				biblioteca.setAttribute("denominazione", denominazione);
				if(bibs.getString("fonte") != null)
				{
					biblioteca.setAttribute("fonte", trim(bibs.getString("fonte")));
				}
				if(bibs.getString("url-fonte") != null)
				{
					biblioteca.setAttribute("url-fonte", bibs.getString("url-fonte"));
				}
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				boolean ok = false;
				while(bib.next())
				{
					ok = true;
					nome = bib.getString("nome");
					categoria = bib.getString("categoria");
					totalePosseduto = bib.getInt("quantita");
// acquistiUltimoAnno = bib.getInt("acquisti-ultimo-anno");
					patrimonioElement = new Element("materiale");
					patrimonioElement.setAttribute("categoria", categoria);
					if(totalePosseduto != 0)
					{
						patrimonioElement.setAttribute("posseduto", "" + totalePosseduto);
					}
					if(acquistiUltimoAnno != 0)
					{
						patrimonioElement.setAttribute("acquisti-ultimo-anno", ""
								+ acquistiUltimoAnno);
					}
					patrimonioElement.setText(nome);
					biblioteca.addContent(patrimonioElement);
				}
				if(ok) root.addContent(biblioteca);
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione patrimonio terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return doc;
	}

	public Document fondiSpeciali()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		stmt = db.prepare(qconfig.getProperty("fondi-speciali.query"));
		bibs = db.select(qconfig.getProperty("censite.query"));
		String descrizione, dewey, deweyTesto;
		Document doc = new Document();
		Element root = new Element("biblioteche");
		root.setAttribute("data-export", dateStampFormat.format(new Date()));
		Element biblioteca;
		Element element;
		doc.setRootElement(root);
		int limit = Integer.MAX_VALUE;
		log.info("Elaborazione fondi speciali");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
		}
		catch(NumberFormatException e)
		{
			log.warn("Massimo numero di biblioteche da elaborare ignorato, si userà il massimo intero possibile");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute(labelIsil, bibs.getString("isil"));
				biblioteca.setAttribute("denominazione",
						bibs.getString("denominazione"));
				if(bibs.getString("fonte") != null)
				{
					biblioteca.setAttribute("fonte", trim(bibs.getString("fonte")));
				}
				stmt.setInt(1, bibs.getInt("id"));
				bib = stmt.executeQuery();
				boolean ok = false;
				while(bib.next())
				{
					ok = true;
					descrizione = bib.getString("descrizione");
					dewey = bib.getString("dewey");
					deweyTesto = bib.getString("dewey-testo");
					element = new Element("fondo-speciale");
					Element denominazione = new Element("denominazione");
					denominazione.setText(bib.getString("denominazione").trim());
					element.addContent(denominazione);
					if(descrizione != null && descrizione.trim() != "")
					{
						element.addContent(new Element("descrizione").setText(descrizione
								.trim()));
					}
					if(dewey != null && dewey.trim() != "")
					{
						if(dewey.length() > 3)
						{
							dewey = dewey.substring(0, 3) + "." + dewey.substring(3);
						}
						Element deweyE = new Element("dewey");
						deweyE.setAttribute("codice", dewey);
						deweyE.setText(deweyTesto.trim());
						element.addContent(deweyE);
					}
					biblioteca.addContent(element);
				}
				if(ok) root.addContent(biblioteca);
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione fondi speciali terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return doc;
	}

	public Document contatti()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		stmt = db.prepare(qconfig.getProperty("contatti.query"));
		log.debug("Query: " + qconfig.getProperty("censite.query"));
		bibs = db.select(qconfig.getProperty("censite.query"));
		String contatto, tipo, note;
		Document doc = new Document();
		Element root = new Element("biblioteche");
		root.setAttribute("data-export", dateStampFormat.format(new Date()));
		Element biblioteca;
		Element contattoElement;
		doc.setRootElement(root);
		int limit = Integer.MAX_VALUE;
		log.info("Elaborazione contatti");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
		}
		catch(NumberFormatException e)
		{
			log.warn("Massimo numero di biblioteche da elaborare ignorato, si userà il massimo intero possibile");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute(labelIsil, bibs.getString("isil"));
				biblioteca.setAttribute("denominazione",
						bibs.getString("denominazione"));
				if(bibs.getString("fonte") != null)
				{
					biblioteca.setAttribute("fonte", trim(bibs.getString("fonte")));
				}
				if(bibs.getString("url-fonte") != null)
				{
					biblioteca.setAttribute("url-fonte", bibs.getString("url-fonte"));
				}
				stmt.setInt(1, bibs.getInt("id"));
				bib = stmt.executeQuery();
				boolean ok = false;
				while(bib.next())
				{
					ok = true;
					contatto = bib.getString("contatto");
					tipo = bib.getString("tipo");
					note = bib.getString("note");
					contattoElement = new Element("contatto");
					contattoElement.setAttribute("tipo", tipo.toLowerCase());
					if(note != null && note.trim() != "")
					{
						contattoElement.setAttribute("note", note.trim());
					}
					contattoElement.setText(contatto.trim());
					biblioteca.addContent(contattoElement);
				}
				if(ok) root.addContent(biblioteca);
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione contatti terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return doc;
	}

	public String tipologie()
	{
		String query = qconfig.getProperty("tipologie.query");
		ResultSet rs = db.select(query);
		ResultSetMetaData rsmd;
		StringWriter output = new StringWriter();
		PrintWriter pw;
		log.info("Elaborazione tipologie");
		partialStart = System.nanoTime();
		try
		{
			pw = new PrintWriter(output);
			rsmd = rs.getMetaData();
			int columns = rsmd.getColumnCount();
			int i;
			String header = csvBOM;
			String row = "";
			String cell = "";
			for(i = 1; i < columns; i++)
			{
				header += csvTS + rsmd.getColumnLabel(i) + csvTS + csvFS;
			}
			header += csvTS + rsmd.getColumnLabel(i) + csvTS;
			pw.println(header);
			while(rs.next())
			{
				row = "";
				for(i = 1; i < columns; i++)
				{
					cell = rs.getString(i);
					if(cell == null)
					{
						cell = "";
					}
					row += csvTS + trim(cell) + csvTS + csvFS;
				}
				row += csvTS + trim(rs.getString(i)) + csvTS;
				pw.println(row);
			}
			pw.close();
		}
		catch(SQLException e)
		{
			log.error("Errore SQL: " + e.getMessage());
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione tipologie terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return output.getBuffer().toString();
	}

	public Document dewey()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		stmt = db.prepare(qconfig.getProperty("specializzazioni.query"));
		bibs = db.select(qconfig.getProperty("censite.query"));
		String dewey, deweyTesto;
		Document doc = new Document();
		Element root = new Element("biblioteche");
		Element biblioteca;
		Element element;
		doc.setRootElement(root);
		int limit = Integer.MAX_VALUE;
		log.info("Elaborazione specializzazioni");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
		}
		catch(NumberFormatException e)
		{
			log.warn("Massimo numero di biblioteche da elaborare ignorato, si userà il massimo intero possibile");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute("isil", bibs.getString("isil"));
				biblioteca.setAttribute("denominazione",
						bibs.getString("denominazione"));
				stmt.setInt(1, bibs.getInt("id"));
				bib = stmt.executeQuery();
				boolean ok = false;
				while(bib.next())
				{
					ok = true;
					dewey = bib.getString("codice");
					deweyTesto = bib.getString("dewey-testo");
					element = new Element("specializzazione");
					if(dewey != null && dewey.trim() != "")
					{
						if(dewey.length() > 3)
						{
							dewey = dewey.substring(0, 3) + "." + dewey.substring(3);
						}
						element.setAttribute("codice", dewey);
						element.setText(deweyTesto.trim());
					}
					biblioteca.addContent(element);
				}
				if(ok) root.addContent(biblioteca);
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione specializzazioni terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return doc;
	}

	public void zip(String fileName)
	{
		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		ZipEntry ze;
		FileInputStream fis;
		BufferedInputStream bis = null;

		String zipFileName = fileName.substring(0, fileName.indexOf(".")) + ".zip";
		try
		{
			fos = new FileOutputStream(tempDir + "/" + zipFileName);
			zos = new ZipOutputStream(fos);
			byte[] data = new byte[2048];
			ze = new ZipEntry(fileName);
			fis = new FileInputStream(tempDir + "/" + fileName);
			bis = new BufferedInputStream(fis, 2048);
			zos.putNextEntry(ze);
			int count;
			while((count = bis.read(data, 0, 2048)) != -1)
			{
				zos.write(data, 0, count);
				zos.flush();
			}
			zos.closeEntry();

		}
		catch(NullPointerException e)
		{

		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}

		try
		{
			bis.close();
			zos.flush();
			zos.close();
			fos.close();
		}
		catch(ZipException e)
		{
			// e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}

	}

	public void zip()
	{
		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		ZipEntry ze;
		FileInputStream fis;
		BufferedInputStream bis = null;

		String zipFileName = config.getProperty("tutto.file");
		try
		{
			fos = new FileOutputStream(tempDir + "/" + zipFileName);
		}
		catch(FileNotFoundException e1)
		{
			log.error("File zip non trovato: " + e1.getMessage());
		}
		zos = new ZipOutputStream(fos);

		String[] fileNames = { config.getProperty("territorio.file"),
				config.getProperty("contatti.file"),
				config.getProperty("patrimonio.file"),
				config.getProperty("fondi-speciali.file"),
				config.getProperty("tipologie.file") };

		log.info("Compressione di tutti i file...");
		for(String fileName : fileNames)
		{
			log.info("Compressione di " + fileName);
			try
			{
				byte[] data = new byte[2048];
				ze = new ZipEntry(fileName);
				fis = new FileInputStream(tempDir + "/" + fileName);
				bis = new BufferedInputStream(fis, 2048);
				zos.putNextEntry(ze);
				int count;
				while((count = bis.read(data, 0, 2048)) != -1)
				{
					zos.write(data, 0, count);
					zos.flush();
				}
				zos.closeEntry();
			}
			catch(NullPointerException e)
			{

			}
			catch(FileNotFoundException e)
			{
				log.error("File " + fileName + " non trovato: " + e.getMessage());
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
		}
		try
		{
			bis.close();
			zos.flush();
			zos.close();
			fos.close();
		}
		catch(ZipException e)
		{
			// e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}

	}

	public String territorioOld()
	{
		String query = config.getProperty("territorioOld.query");
		log.debug("Query: " + query);
		ResultSet bib;
		ResultSetMetaData rsmd;
		StringWriter output = new StringWriter();
		PrintWriter pw;
		String isil = "", row = null, cell, contatto, note;
		String tel = "", fax = "", mail = "", url = "";
		String oldIsil = "";
		int tipo;
		int limit = Integer.MAX_VALUE;
		int columns = 0;
		int i;
		log.info("Elaborazione territorio");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
		}
		catch(NumberFormatException e)
		{
			log.warn("Massimo numero di biblioteche da elaborare ignorato, si userà il massimo intero possibile");
		}
		try
		{
			boolean headerOk = false;
			pw = new PrintWriter(output);
			bib = db.select(query);
			while(bib.next() && limit-- > 0)
			{
				isil = bib.getString(1);
				try
				{

					// una sola volta si crea l'header

					if(!headerOk)
					{
						rsmd = bib.getMetaData();
						columns = rsmd.getColumnCount() - 3;
						String header = csvBOM;
						row = "";
						cell = "";
						for(i = 1; i < columns; i++)
						{
							header += wrap(rsmd.getColumnLabel(i));
						}
						header += wrap(rsmd.getColumnLabel(i));

						// si aggiungono all'header quattro campi che saranno riempiti in
						// base ai tipi di contatti rinvenuti

						header += wrap("telefono");
						header += wrap("fax");
						header += wrap("email");
						header += wrap("url", true);
						pw.println(header);
						headerOk = true;
					}

					if(!isil.equals(oldIsil))
					{
						if(oldIsil != "")
						{
							row += wrap(tel) + wrap(fax) + wrap(mail) + wrap(url, true);
							pw.println(row);
							pw.flush();
						}
						row = "";
						for(i = 1; i < columns; i++)
						{
							cell = bib.getString(i);
							if(cell == null)
							{
								cell = "";
							}
							row += wrap(cell.trim());
						}
						row += wrap(bib.getString(i));
						oldIsil = isil;
						tel = fax = mail = url = "";
					}

					// vanno gestiti i possibili contatti
					contatto = bib.getString("contatto");

					note = bib.getString("note");
					tipo = bib.getInt("tipo");
					if(contatto != null)
					{
						contatto = contatto.trim();
						if(note == null || note.trim() == "")
						{
							/*
							 * i contatti vanno selezionati per codice, perché il right join
							 * non funziona se si estraggono anche i codici e le descrizioni
							 */
							switch(tipo)
							{
								case 1:
									// telefono
									if(tel == "") tel = contatto;
									break;
								case 2:
									// fax
									if(fax == "") fax = contatto;
									break;
								case 3:
									// mail
									if(mail == "") mail = contatto;
									break;
								case 5:
									// url
									if(url == "") url = contatto;
									break;
								default:
									break;
							}
						}
					}
				}
				catch(SQLException e)
				{
					log.error("Errore SQL: " + e.getMessage());
				}
			}
			pw.close();
		}
		catch(SQLException e)
		{
			log.error("Errore SQL: " + e.getMessage());
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione territorio terminata in "
				+ (partialStop - partialStart) / 1000000000 + " secondi");
		return output.getBuffer().toString();
	}

	public static void main(String[] args)
	{
		OpenData od = new OpenData();
		od.log.info("Creazione file in formati open data");
		System.gc();
		od.totalStart = System.nanoTime();
		try
		{
			XMLOutputter xo = new XMLOutputter(Format.getPrettyFormat());
			Document doc;
			PrintWriter pw;
			String tDir = od.tempDir + "/";
			String tFile;
			if(od.config.getProperty("territorio") != null)
			{
				tFile = od.config.getProperty("territorio.file");
				pw = new PrintWriter(tDir + tFile);
				pw.println(od.territorio());
				pw.close();
				od.zip(tFile);
			}
			if(od.config.getProperty("contatti") != null)
			{
				doc = od.contatti();
				tFile = od.config.getProperty("contatti.file");
				pw = new PrintWriter(tDir + tFile);
				xo.output(doc, pw);
				od.zip(tFile);
			}
			if(od.config.getProperty("patrimonio") != null)
			{
				doc = od.patrimonio();
				tFile = od.config.getProperty("patrimonio.file");
				pw = new PrintWriter(tDir + tFile);
				xo.output(doc, pw);
				od.zip(tFile);
			}
			if(od.config.getProperty("fondi-speciali") != null)
			{
				doc = od.fondiSpeciali();
				tFile = od.config.getProperty("fondi-speciali.file");
				pw = new PrintWriter(tDir + tFile);
				xo.output(doc, pw);
				od.zip(tFile);
			}
			if(od.config.getProperty("tipologie") != null)
			{
				tFile = od.config.getProperty("tipologie.file");
				pw = new PrintWriter(tDir + tFile);
				pw.println(od.tipologie());
				pw.close();
				od.zip(tFile);
			}
			if(od.config.getProperty("specializzazioni") != null)
			{
				doc = od.dewey();
				tFile = od.config.getProperty("specializzazioni.file");
				pw = new PrintWriter(tDir + tFile);
				xo.output(doc, pw);
				od.zip(tFile);
			}
			od.zip();
		}
		catch(FileNotFoundException e)
		{
			od.log.error("File non trovato: " + e.getMessage());
		}
		catch(IOException e)
		{
			od.log.error("Errore di I/O: " + e.getMessage());
		}
		od.totalStop = System.nanoTime();
		od.log.info("Esecuzione terminata in " + (od.totalStop - od.totalStart)
				/ 1000000000 + " secondi");
	}
}
