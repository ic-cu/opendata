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
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.TreeMap;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class OpenData
{
	private DB db;
	private Properties config, dbconfig, qconfig;
	private String today;
	private String tempDir;
	private String csvFS, csvTS, csvBOM;
	private Logger log;
	private long totalStart, totalStop, partialStart, partialStop;
	private SimpleDateFormat dateStampFormat, dateFormat;
	private String labelIsil, labelSbn;
	private TreeMap<Integer, String> isilMap, statiMap;

/*
 * Alcuni oggetti relativi all'output JSON, che viene creato durante tutto il
 * resto del processo ed estratto solo al termine di tutte le procedure, se le
 * risorse sono sufficienti.
 */

	private JsonObject jExport;
	private JsonArray jBibs;
	private Gson gson;

	public String getJson()
	{
		return gson.toJson(jExport);
	}

/*
 * Metodo per ripulire una stringa da sporcizia varia, tipicamente spazi
 * multipli, a capo, spazi iniziali o finali
 */

	private String clear(String s)
	{
		if(s != null)
		{
			s = s.replaceAll("\n", " ");
			s = s.replaceAll(" +", " ");
		}
		else
		{
			s = "";
		}
		return s.trim();
	}

	private String wrap(String field, boolean last)
	{
		field = clear(field);
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

/*
 * Dati frequentemente usati sono caricati in apposite mappe ordinate, tutte
 * indicizzate per id della biblioteca
 */
	
	private void loadMaps()
	{
		ResultSet rs;
		String q;
		String isil, stato;
		int id;
		try
		{
			isilMap = new TreeMap<Integer, String>();
			q = qconfig.getProperty("tutte.query");
			log.debug("Query tutte: " + q);
			rs = db.select(q);
			while(rs.next())
			{
				id = rs.getInt("id");
				isil = rs.getString("isil");
				isilMap.put(id, isil);
			}
			statiMap = new TreeMap<Integer, String>();
			q = qconfig.getProperty("stati.catalogazione.query");
			log.debug("Query stati: " + q);
			rs = db.select(q);
			while(rs.next())
			{
				id = rs.getInt("id");
				stato = rs.getString("stato");
				isil = rs.getString("isil confluita");
				if(isil != null) 
				{
					stato += " in " + isil; 
				}
				statiMap.put(id, stato);
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
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
			log.error("Impossibile leggere il file di configurazione: " + e.getMessage());
		}
		String url = dbconfig.getProperty("db.url");
		String user = dbconfig.getProperty("db.user");
		String pass = dbconfig.getProperty("db.pass");
		db = new DB(DB.mysqlDriver, url, user, pass);

		SimpleDateFormat sdf;
		sdf = new SimpleDateFormat("yyyyMMdd");
		today = sdf.format(new Date());

		dateStampFormat = new SimpleDateFormat(config.getProperty("dateStamp.pattern"));
		dateFormat = new SimpleDateFormat(config.getProperty("date.pattern"));

		tempDir = config.getProperty("temp.dir");

		if(config.getProperty("temp.dir.daily") != null)
		{
			tempDir += "/" + today;
		}
		File tDir = null;
		tDir = new File(tempDir);
		tDir.mkdirs();
		jExport = new JsonObject();
		gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().serializeNulls().create();

		csvFS = config.getProperty("csv.fs");
		csvTS = config.getProperty("csv.ts");
		csvBOM = config.getProperty("csv.bom");
		labelIsil = config.getProperty("label.xml.isil");
		labelSbn = config.getProperty("label.xml.sbn");
		log.info("Separatore campi per formato CSV [" + csvFS + "]");
		log.info("Separatore testo per formato CSV [" + csvTS + "]");
		loadMaps();
	}

	public String territorio()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		String query = qconfig.getProperty("territorio.query");
		log.debug("Query: " + query);
		stmt = db.prepare(query);
		bibs = db.select(qconfig.getProperty("censite.query"));
		log.debug("Query censite: " + qconfig.getProperty("censite.query"));
		String isil, sbn, denominazione;
		int idBib;
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
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
		}
		partialStart = System.nanoTime();
		try
		{
			boolean headerOk = false;
			while(bibs.next() && limit > 0)
			{
				limit--;
				isil = bibs.getString("isil");
				sbn = bibs.getString("sbn");
				idBib = bibs.getInt("id");
				denominazione = bibs.getString("denominazione");
				pw = new PrintWriter(output);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				while(bib.next() && limit-- > 0)
				{
					log.debug("Elaborazione " + isil);
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
							header += wrap(labelSbn);
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
							header += wrap("url", true);
							pw.println(header);
							headerOk = true;
						}

						if(!isil.equals(oldIsil))
						{
							log.debug(denominazione);
							if(oldIsil != "")
							{
								row += wrap(tel) + wrap(fax) + wrap(mail) + wrap(url, true);
								log.debug("oldISIL non null: " + row);
								pw.println(row);
								pw.flush();
							}
							row = "";
							row += wrap(isil) + wrap(sbn) + wrap(denominazione);
							for(i = 1; i < columns; i++)
							{
								cell = bib.getString(i);
								if(cell == null)
								{
									cell = "";
								}
								row += wrap(cell.trim());
							}
							log.debug("Nuova riga: " + row);
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
			}
			row += wrap(tel) + wrap(fax) + wrap(mail) + wrap(url, true);
			log.debug("Ultimo ISIL: " + row);
			pw.print(row);
			pw.flush();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		pw.close();
		partialStop = System.nanoTime();
		log.info("Elaborazione territorio terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
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
		root.setAttribute("data-export", dateStampFormat.format(new Date()).replaceFirst("[0-9][0-9]$", ""));
		Element biblioteca;
		Element patrimonioElement;
		doc.setRootElement(root);
		int limit = Integer.MAX_VALUE;
		log.info("Elaborazione patrimonio");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
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
						patrimonioElement.setAttribute("acquisti-ultimo-anno", "" + acquistiUltimoAnno);
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
		log.info("Elaborazione patrimonio terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
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
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute(labelIsil, bibs.getString("isil"));
				biblioteca.setAttribute("denominazione", bibs.getString("denominazione"));
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
						element.addContent(new Element("descrizione").setText(descrizione.trim()));
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
		log.info("Elaborazione fondi speciali terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
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
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute(labelIsil, bibs.getString("isil"));
				if(bibs.getString("sbn") != null)
				{
					biblioteca.setAttribute(labelSbn, bibs.getString("sbn"));
				}
				biblioteca.setAttribute("denominazione", bibs.getString("denominazione"));
				stmt.setInt(1, bibs.getInt("id"));
				bib = stmt.executeQuery();
				boolean ok = false;
				while(bib.next())
				{
					ok = true;
					contatto = clear(bib.getString("contatto"));
					tipo = bib.getString("tipo");
					note = clear(bib.getString("note"));
					contattoElement = new Element("contatto");
					contattoElement.setAttribute("tipo", tipo.toLowerCase());
					if(note != null && note.trim() != "")
					{
						contattoElement.setAttribute("note", note);
					}
					contattoElement.setText(contatto);
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
		log.info("Elaborazione contatti terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
		return doc;
	}

	public String tipologie()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		String query = qconfig.getProperty("tipologie.query");
		log.debug("Query: " + query);
		stmt = db.prepare(query);
		bibs = db.select(qconfig.getProperty("censite.query"));
		String isil;
		int idBib;
		ResultSetMetaData rsmd;
		StringWriter output = new StringWriter();
		PrintWriter pw;
		int limit = Integer.MAX_VALUE;
		log.info("Elaborazione tipologie");
		String header = csvBOM;
		String row = "";
		String cell = "";
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
		}
		partialStart = System.nanoTime();
		try
		{
			pw = new PrintWriter(output);
			boolean headerOk = false;
			while(bibs.next() && limit > 0)
			{
				limit--;
				isil = bibs.getString("isil");
				idBib = bibs.getInt("id");
				pw = new PrintWriter(output);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				rsmd = bib.getMetaData();
				int columns = rsmd.getColumnCount();
				int i;
				while(bib.next() && limit-- > 0)
				{
					log.debug("Elaborazione " + isil);
					try
					{

// una sola volta si crea l'header

						if(!headerOk)
						{
							for(i = 1; i < columns; i++)
							{
								header += csvTS + rsmd.getColumnLabel(i) + csvTS + csvFS;
							}
							header += csvTS + rsmd.getColumnLabel(i) + csvTS;
							pw.println(header);
							headerOk = true;
						}
						row = "";
						for(i = 1; i < columns; i++)
						{
							cell = bib.getString(i);
							if(cell == null)
							{
								cell = "";
							}
							row += csvTS + cell.trim() + csvTS + csvFS;
						}
						row += csvTS + bib.getString(i) + csvTS;
						pw.println(row);
					}
					catch(SQLException e)
					{
						log.error("Errore SQL: " + e.getMessage());
					}
					pw.close();
				}
			}
		}
		catch(SQLException e)
		{
			log.error("Errore SQL: " + e.getMessage());
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione tipologie terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
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
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
		}
		try
		{
			while(bibs.next() && limit > 0)
			{
				limit--;
				biblioteca = new Element("biblioteca");
				biblioteca.setAttribute("isil", bibs.getString("isil"));
				biblioteca.setAttribute("denominazione", bibs.getString("denominazione"));
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
		log.info("Elaborazione specializzazioni terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
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

		String[] fileNames = { config.getProperty("territorio.file"), config.getProperty("contatti.file"),
				config.getProperty("patrimonio.file"), config.getProperty("fondi-speciali.file"), config.getProperty("tipologie.file"),
				config.getProperty("json.file") };

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

	public void json()
	{
		ResultSet bibs;
		ResultSet bib;
		ResultSet tipiCodiciRS;
		PreparedStatement stmt = null;
		String query = null;
		String isil = null;
		int idBib = 0;
		int count = 0;
		log.debug("Query: " + query);
		bibs = db.select(qconfig.getProperty("tutte.query"));
		int limit = Integer.MAX_VALUE;
		log.info("Estrazione formato JSON");
		partialStart = System.nanoTime();
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
		}
		try
		{
			jBibs = new JsonArray();
			while(bibs.next() && limit > 0)
			{
				limit--;
				count++;
				idBib = bibs.getInt("id");
				JsonObject jBib = new JsonObject();
				Date dCen = bibs.getDate("data-censimento");
				Date dAgg = bibs.getDate("data-aggiornamento");
				if(dCen != null)
				{
					jBib.addProperty("anno-censimento", dateFormat.format(dCen).substring(0, 4));
				}
				else
				{
					jBib.addProperty("anno-censimento", (String) null);
				}
				if(dAgg != null)
				{
					jBib.addProperty("data-aggiornamento", dateFormat.format(dAgg));
				}
				else
				{
					jBib.addProperty("data-aggiornamento", (String) null);
				}

/*
 * Codici vari, raggruppati in un array di coppie. Per estrarre comunque anche
 * codici null si costruisce prima un vettore di tipi di codice su cui ciclare
 */

				query = qconfig.getProperty("tipi.codici.query");
				log.debug(query);
				tipiCodiciRS = db.select(query);
				ArrayList<String> tipiCodici = new ArrayList<String>();
				while(tipiCodiciRS.next())
				{
					tipiCodici.add(tipiCodiciRS.getString(2));
				}

// Il primo codice da aggiungere è sempre l'ISIL, poi si aggiungono
// eventualmente gli altri. Si preferisce un oggetto con un fissato numero di
// proprietà piuttosto che un array

				JsonObject jCodici = new JsonObject();
//				isil = bibs.getString("isil");
				isil = isilMap.get(idBib);
				jCodici.addProperty("isil", isil);

				query = qconfig.getProperty("codici.query");
				stmt = db.prepare(query);

// Per ogni tipo di codice si cerca un eventuale valore per la singola
// biblioteca.

				for(int i = 0; i < tipiCodici.size(); i++)
				{
					stmt.setInt(1, i + 1);
					stmt.setInt(2, idBib);
					bib = stmt.executeQuery();
					String tipo = tipiCodici.get(i);
					String codice = null;

// Se il resultset ha un elemento per il tipo attuale, teoricamente unico, si
// aggiunge la coppia (tipo, valore), altrimenti la coppia (tipo, null),
// essendo null il valore con cui è inizializzata la variabile "codice"

					if(bib.next())
					{
						codice = bib.getString("codice");
						log.debug(tipo + " " + codice);
					}
					jCodici.addProperty(tipo, codice);
				}
				jBib.add("codici-identificativi", jCodici);

/*
 * Cominciano con le denominazioni. Quella ufficiale è già nella query "censite"
 */

				JsonObject jNomi = new JsonObject();
				jNomi.addProperty("ufficiale", bibs.getString("denominazione"));

// Denominazioni precedenti, si aggiungeranno a jNomi

				query = qconfig.getProperty("denominazioni.precedenti.query");
				log.debug(query);
				stmt = db.prepare(query);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				JsonArray jNomiPrecedenti = new JsonArray();
				while(bib.next())
				{
					jNomiPrecedenti.add(new JsonPrimitive(bib.getString("denominazione")));
				}
				jNomi.add("precedenti", jNomiPrecedenti);

// Denominazioni alternative, trattate come le precedenti

				query = qconfig.getProperty("denominazioni.alternative.query");
				stmt = db.prepare(query);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				JsonArray jNomiAlternativi = new JsonArray();
				while(bib.next())
				{
					jNomiAlternativi.add(new JsonPrimitive(bib.getString("denominazione")));
				}
				jNomi.add("alternative", jNomiAlternativi);

// La proprietà "denominazioni" si aggiunge come array a jBib

				jBib.add("denominazioni", jNomi);

// Ora si cicla su un resultset molto ampio

				query = qconfig.getProperty("json.query");
				stmt = db.prepare(query);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();

// Si comincia il ciclo da tipologie ed ente di appartenenza, che però saranno
// aggiunti in fondo

				String tipAmm = null, ente = null, tipFunz = null;
				JsonObject jAccesso = new JsonObject();
				while(bib.next())
				{
					tipAmm = bib.getString("tipologia amministrativa");
					ente = bib.getString("denominazione ente");
					tipFunz = bib.getString("tipologia funzionale");

// Apertura in generale e a portatori di handicap. Il caso NULL è delicato e va
// esplicitato, altrimenti si confonde con il false. Anche questi dati sono
// aggiunti alla fine

					if(bib.getString("riservata") != null)
					{
						jAccesso.addProperty("riservato", bib.getBoolean("riservata"));
					}
					else
					{
						jAccesso.addProperty("riservato", bib.getString("riservata"));
					}

					if(bib.getString("handicap") != null)
					{
						jAccesso.addProperty("portatori-handicap", bib.getBoolean("handicap"));
					}
					else
					{
						jAccesso.addProperty("portatori-handicap", bib.getString("handicap"));
					}

// Indirizzo, come contenitore

					JsonObject jIndirizzo = new JsonObject();

					jIndirizzo.addProperty("via-piazza", bib.getString("indirizzo"));
					jIndirizzo.addProperty("frazione", bib.getString("frazione"));
					jIndirizzo.addProperty("cap", bib.getString("cap"));
					JsonObject jComune = new JsonObject();
					jComune.addProperty("nome", bib.getString("comune"));
					jComune.addProperty("istat", bib.getString("codice istat comune"));
					jIndirizzo.add("comune", jComune);
					JsonObject jProvincia = new JsonObject();
					jProvincia.addProperty("nome", bib.getString("provincia"));
					jProvincia.addProperty("istat", bib.getString("codice istat provincia"));
					jProvincia.addProperty("sigla", bib.getString("sigla"));
					jIndirizzo.add("provincia", jProvincia);
					jIndirizzo.addProperty("regione", bib.getString("regione"));

// Coordinate

					JsonArray jCoordinate = new JsonArray();
					String lat = bib.getString("latitudine");
					String lon = bib.getString("longitudine");
					if(lat != null && lat != "" && lon != null && lon != "")
					{
						jCoordinate.add(new JsonPrimitive(Double.parseDouble(lat.replace(",", "."))));
						jCoordinate.add(new JsonPrimitive(Double.parseDouble(lon.replace(",", "."))));
					}
					else
					{
						log.warn(isil + ": una delle coordinate è vuota o null");
					}
					jIndirizzo.add("coordinate", jCoordinate);
					jBib.add("indirizzo", jIndirizzo);
				}

// Contatti

				query = qconfig.getProperty("contatti.query");
				stmt = db.prepare(query);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				JsonArray jContatti = new JsonArray();

				while(bib.next())
				{
					JsonObject jContatto = new JsonObject();
					jContatto.addProperty("tipo", bib.getString("tipo"));
					jContatto.addProperty("valore", bib.getString("contatto"));
					jContatto.addProperty("note", bib.getString("note"));
					jContatti.add(jContatto);
				}
				jBib.add("contatti", jContatti);
				jBib.add("accesso", jAccesso);
				jBib.addProperty("tipologia-amministrativa", tipAmm);
				jBib.addProperty("tipologia-funzionale", tipFunz);
				jBib.addProperty("ente", ente);

// Estrae l'eventuale stato di catalogazione				
								
				jBib.addProperty("stato-registrazione", statiMap.get(idBib));

// Dopo aver completato una biblioteca, si aggiunge a tutte le altre.

				jBibs.add(jBib);
			}

// Si aggiunge all'export prima il numero di biblioteche, poi i loro dati

			JsonObject jMeta = new JsonObject();
			jMeta.addProperty("data-estrazione", dateStampFormat.format(new Date()));
			jMeta.addProperty("biblioteche-estratte", count);
			jExport.add("metadati", jMeta);
			jExport.add("biblioteche", jBibs);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		partialStop = System.nanoTime();
		log.info("Elaborazione contatti terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
	}

	public String indirizzi()
	{
		ResultSet bibs;
		ResultSet bib;
		PreparedStatement stmt;
		String query = qconfig.getProperty("indirizzi.query");
		log.debug("Query: " + query);
		stmt = db.prepare(query);
		bibs = db.select(qconfig.getProperty("tutte.query"));
		log.debug("Query censite: " + qconfig.getProperty("tutte.query"));
		String isil, denominazione;
		int idBib;
		ResultSetMetaData rsmd;
		StringWriter output = new StringWriter();
		PrintWriter pw = null;
		String row = null, cell;
		String oldIsil = "";
		int limit = Integer.MAX_VALUE;
		int columns = 0;
		int i;
		log.info("Elaborazione territorio");
		try
		{
			limit = Integer.parseInt(config.getProperty("censite.limit"));
			log.warn("Elaborazione delle prime " + limit + " biblioteche");
		}
		catch(NumberFormatException e)
		{
			log.warn("Elaborazione di tutte le biblioteche");
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
				pw = new PrintWriter(output);
				stmt.setInt(1, idBib);
				bib = stmt.executeQuery();
				while(bib.next() && limit-- > 0)
				{
					log.debug("Elaborazione " + isil);
					try
					{

						// una sola volta si crea l'header

						if(!headerOk)
						{
							rsmd = bib.getMetaData();
							columns = rsmd.getColumnCount();
							String header = csvBOM;
							row = "";
							cell = "";
							header += wrap(labelIsil);
							header += wrap("denominazione");
							for(i = 1; i < columns; i++)
							{
								header += wrap(rsmd.getColumnLabel(i));
							}
							header += wrap(rsmd.getColumnLabel(i), true);
							pw.println(header);
							headerOk = true;
						}

						if(!isil.equals(oldIsil))
						{
							log.debug(denominazione);
							if(oldIsil != "")
							{
// row += csvTS;
								log.debug("oldISIL non null: " + row);
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
							log.debug("Nuova riga: " + row);
							row += wrap(bib.getString(i), true);
							oldIsil = isil;
						}

					}
					catch(SQLException e)
					{
						log.error("Errore SQL: " + e.getMessage());
					}
				}
			}
			row += csvTS;
			log.debug("Ultimo ISIL: " + row);
			pw.print(row);
			pw.flush();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		pw.close();
		partialStop = System.nanoTime();
		log.info("Elaborazione territorio terminata in " + (partialStop - partialStart) / 1000000000 + " secondi");
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
				pw.print(od.territorio());
				pw.close();
				od.zip(tFile);
			}
			if(od.config.getProperty("indirizzi") != null)
			{
				tFile = od.config.getProperty("indirizzi.file");
				pw = new PrintWriter(tDir + tFile);
				pw.print(od.indirizzi());
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
			if(od.config.getProperty("json") != null)
			{
				tFile = od.config.getProperty("json.file");
				pw = new PrintWriter(tDir + tFile);
				od.json();
				pw.println(od.getJson());
				pw.close();
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
		od.log.info("Esecuzione terminata in " + (od.totalStop - od.totalStart) / 1000000000 + " secondi");
	}
}
