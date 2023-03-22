package src.Other;

import Graph.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class DBMutation {
	/*
	TODO fare un qualcosa che dato un valore da inserire in sql, ritorna una stringa con il corretto inserimento per sql
	In base al data type*/
	private static Scanner tastiera = new Scanner(System.in).useDelimiter("\n");
	private static String risp="";
	public static boolean run;
	private static String query="";
	private static Statement st = null;
	private static ResultSet rs = null;
	private static String val = "";
	private static String tipo = "";
	private static String cond = "";
	private static Graph graph;
	public static void mutation(String db, Graph DBGraph) {
		String tabella = "";
		String colonna = "";
		String op = "";
		graph = DBGraph;
		risp="si";
		try {
			DBConnection.getConn().createStatement().execute("Use " + db);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
		while(risp.equalsIgnoreCase("si")) {
			System.out.println("Tabella:");
			tabella = tastiera.nextLine();

			if(MainDebug.graph.listTables().contains(tabella)) {
				if (MainDebug.graph.getColumnNumberInTable(tabella) != MainDebug.graph.getPrimaryKeyNumberInTable(tabella)) {

					while (risp.equalsIgnoreCase("si")) {
						System.out.println("Colonna (non key/index):");
						colonna = tastiera.nextLine();
						Column column = MainDebug.graph.searchColumnInTable(colonna, tabella);
						//TODO implementare che la modifica di una foreign key � possibile entro i limiti delle PK che la referenziano
						if (column != null && !column.isPrimaryKey() && !(column instanceof ForeignKeyColumn)) {
							//scelta operazione (blank-modify)
							if (!column.isNullable()) {
								System.out.println("Unica operazione possibile: Modify.\nVuoi eseguirla?");
								op = risposta();
								if (op.equalsIgnoreCase("si"))
									op = "modify";
								else
									op = null;
							} else {
								System.out.println("Inserici quale operazione eseguire (blank - modify):");
								op = tastiera.nextLine();
							}

							//Se op � null allora non vuole svolgere solo modify ed esce
							if (op != null) {
								if (op.equalsIgnoreCase("blank")) {
									blank(column, tabella);
								} else if (op.equalsIgnoreCase("modify")) {
									modify(column, tabella);

								} else {
									System.out.println("Errore, operazione inserita non valida");
								}
							}
						} else {
							System.out.println("Colonna inserita non presente, � primary Key o ForeignKey");
						}
						System.out.println("Eseguire altre modifiche alla tabella: " + tabella + "?(si/no)");
						risp = risposta();
					}
				} else {
					System.out.println("Tabella con solo primary keys: non modificabile");
				}
				System.out.println("Eseguire altre modifiche al db:" +db+"?(si/no)");
				risp = risposta();
			}else {
				System.out.println("Riprovare? (si/no)");
				risp = risposta();
			}
		}
		System.out.println("Fine modifiche");
	}
			
	/*Metodo per il riempimento dei blank
	 * - conta il numero di blank presenti nella colonna (se non presenti finisce l'esecuzione)
	 * - scelta tipologia di riempimento
	 * - invocazione del rispettivo metodo
	 */ 
	public static void blank(Column column, String tabella) {
		int n_blank = -1;
		switch(column.getDatatype()) {
			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR -> {
				query = "SELECT COUNT(*) FROM " + tabella + " WHERE " + column.getName() + " IS NULL OR " + column.getName() + "=''";
			}

			default -> {
					query = "SELECT COUNT(*) FROM " + MainDebug.DB + "." + tabella + " WHERE " + column.getName() + " IS NULL";
			}
		}
		try {
			st = DBConnection.getConn().createStatement();
			rs = st.executeQuery(query);
			if(rs.next()) {
				n_blank = rs.getInt(1);
			}
		}catch(SQLException se) {
			System.out.println("Error: "+se.getMessage());
		}finally {
			DBConnection.closeSt(st);
			DBConnection.closeRs(rs);
		}
		if(n_blank == 0) {
			System.out.println("Non sono presenti spazi vuoti in: "+column.getName());
		}else if(n_blank > 0) {
			while(risp.equalsIgnoreCase("si")) {
				System.out.println("Inserire riempimento (average - input - random)");
				tipo = tastiera.nextLine();
				if(tipo.equalsIgnoreCase("average")) {
					average(column,tabella, n_blank);
					risp="no";
				}else if(tipo.equalsIgnoreCase("input")) {
					inputB(column, tabella);
					risp="no";
				}else if(tipo.equalsIgnoreCase("random")) {
					random(column,tabella);
					risp="no";
				}else {
					System.out.println("Tipo inserito non valido, riprovare? (si/no)");
					risp = risposta();
				}
			}
		}						
		System.out.println("Fine, blank");
	}
	
	/*Metodo per riepimento: AVERAGE
	 * - se la colonna � di tipo numerico, riempie con la media
	 * - se la colonna � di tipo stringa/altro, riempie con la pi� frequente
	 */
	public static void average(Column column, String tabella, int n_blank) {
		Random r = new Random();
		int remaining = n_blank;
		switch(column.getDatatype()) {
			//TODO mettere tipi numerici
			case Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.DECIMAL,
					Types.FLOAT, Types.REAL, Types.TINYINT -> {
				try {
					query = "SELECT AVG("+column.getName()+") FROM " +tabella;
					st = DBConnection.getConn().createStatement();
					rs = st.executeQuery(query);

					if(rs.next()) {
						int avg = rs.getInt(1);

						String pKeys = MainDebug.graph.getPrimaryKeysStringInTable(tabella, ',', "", "");
						String parentesys = "";
						if (MainDebug.graph.isPrimaryKeyComposedInTable(tabella))
							parentesys = "(" + pKeys + ")";
						 else
							 parentesys = pKeys;

						while(remaining > 0) {
							int n = r.nextInt(remaining) + 1;
							remaining -= n;
							int val = avg + r.nextInt(avg);
							query = "UPDATE " + tabella + " SET " + column.getName() + "= " + val +
									" WHERE " + column.getName() + " IS NULL and " + parentesys + " IN (" +
									"SELECT " + pKeys + " FROM (" +
									"SELECT " + pKeys + " FROM " + tabella + " " +
									"ORDER BY RAND() LIMIT " + n;
							st.executeUpdate(query);
							System.out.println("Operazione eseguita, " +n+ " blank riempiti con: " + val);
						}
					}
				}catch(SQLException se) {
					System.out.println("Error: " + se.getMessage());
				}finally {
					DBConnection.closeSt(st);
					DBConnection.closeRs(rs);
				}
			}
			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR ->{
				try {

					query = "SELECT " + column.getName() + " " +
							"FROM " + tabella + " " +
							"WHERE " + column.getName() + " IS NOT NULL AND " +
							"" + column.getName() + "!= ''";
					st = DBConnection.getConn().createStatement();
					rs = st.executeQuery(query);

					List<String> vals = new ArrayList<>();
					while(rs.next()){
						vals.add(rs.getString(1));
					}
					while(remaining > 0) {
						int n = 0;
						while(n == 0) {
							n = r.nextInt(remaining) + 1;
						}
						remaining -= n;
						//Grago tiene traccia come trattare chioave esterna e poi usa questa info per deletare
						//la FK tiene traccia di come va deletata e in base a quello poi va a decidere come fare la delete
						String val = vals.get(r.nextInt(vals.size()));
						String pKeys = MainDebug.graph.getPrimaryKeysStringInTable(tabella, ',', "", "");
						String parentesys = "";
						if (MainDebug.graph.isPrimaryKeyComposedInTable(tabella))
							parentesys = "(" + pKeys + ")";
						else
							parentesys = pKeys;
						query = "UPDATE " + tabella + " SET " + column.getName() + "= '" + val + "'" +
								" WHERE " + column.getName() + " IS NULL OR " + column.getName() + "= '' and " +
								parentesys + " IN (" +
								"SELECT " + pKeys + " FROM ( " +
								"SELECT " + pKeys + " FROM " + tabella + " " +
								"ORDER BY RAND() LIMIT " + n +") t)";

						st = DBConnection.getConn().createStatement();
						st.executeUpdate(query);
						System.out.println("Operazione eseguita, " +n+ " blank riempiti con: " + val);

					}
				}catch(SQLException se) {
					System.out.println("Error: " + se.getMessage());
				}finally {
					DBConnection.closeSt(st);
					DBConnection.closeRs(rs);
				}
			} default -> {
				try {
					query = "SELECT " +column.getName()+ ", COUNT("+column.getName()+") AS myCount " +
							"FROM " + tabella+ " " +
							"WHERE "+column.getName()+" IS NOT NULL AND " +
							"" +column.getName()+ "!= '' " +
							"GROUP BY " +column.getName()+ " ORDER BY myCount DESC";
					st = DBConnection.getConn().createStatement();
					rs = st.executeQuery(query);
					if(rs.next()) {
						val = rs.getObject(1).toString();
						query = "UPDATE " +tabella+ " SET "+column.getName()+"= '" +val+ "' WHERE " +column.getName()+" IS NULL";

						st = DBConnection.getConn().createStatement();
						st.executeUpdate(query);
						System.out.println("Operazione eseguita, blank riempiti con: "+val);
					}
				}catch(SQLException se) {
					System.out.println("Error: " + se.getMessage());
				}finally {
					DBConnection.closeSt(st);
					DBConnection.closeRs(rs);
				}
			}


		}
	}
	
	/*Metodo per riepimento: INPUT
	 * - chiede il valore all'utente
	 * - riempie i blank con il valore indicato
	 */
	public static void inputB(Column colonna, String tabella) {
		System.out.println("Inserisci il valore:");
		val = tastiera.nextLine();
		switch(colonna.getDatatype()){
			//TODO check types
			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR ->{
				query = "UPDATE " +tabella+ " SET "+colonna.getName()+"= '" +val+ "' " +
						"WHERE " +colonna.getName()+" IS NULL OR " +colonna.getName()+ "= ''" ;
			}
			default -> {
				query = "UPDATE " +tabella+ " SET "+colonna.getName()+"= " +val+ " WHERE " +colonna.getName()+" IS NULL" ;
			}
		}

		try {
			st = DBConnection.getConn().createStatement();
			st.executeUpdate(query);
			System.out.println("Operazione eseguita, blank riempiti con: "+val);
		}catch(SQLException se) {
			System.out.println("Error: " + se.getMessage());
			se.printStackTrace();
		}finally {
			DBConnection.closeSt(st);
		}		
	}
	
	/*Metodo per riepimento: RANDOM
	 * - prende un valore random presente nel database (della colonna su cui si sta operando)
	 * - riempie i blank con quel valore
	 */
	public static void random(Column colonna, String tabella) {
		try {
			query = "SELECT "+colonna.getName()+ " FROM " + tabella+ "" +
					" WHERE "+colonna.getName()+" IS NOT NULL AND " +
					""+colonna.getName()+"!='' ORDER BY RAND() LIMIT 1";
			st = DBConnection.getConn().createStatement();
			rs = st.executeQuery(query);
			if(rs.next()) {				
				val = rs.getObject(1).toString();
				switch(colonna.getDatatype()) {
					//TODO check types
					case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR -> {
						query = "UPDATE " + tabella + " SET " + colonna + "= '" + val + "' WHERE " + colonna + " IS NULL OR " + colonna + "= ''";
					}
					default -> {
						query = "UPDATE " + tabella + " SET " + colonna + "= " + val + " WHERE " + colonna + " IS NULL";
					}
				}
				st.executeUpdate(query);
				System.out.println("Operazione eseguita, blank riempiti con: "+val);
			}
		}catch(SQLException se) {
			System.out.println("Error: " + se.getMessage());
		}finally {
			DBConnection.closeSt(st);
		}
	}
	
	/*Metodo per la modifica di valori di una colonna
	 * - scelta, in base al tipo di colonna, il tipo di modifica
	 * - invocazione del rispettivo metodo 
	 */ 
	public static void modify(Column column, String tabella) {
		while(risp.equalsIgnoreCase("si")) {
			switch(column.getDatatype()) {
				case Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.DECIMAL,
						Types.FLOAT, Types.REAL, Types.TINYINT -> {
					System.out.println("Inserisci tipo di modifica (operazione - input)");
					tipo = tastiera.nextLine();
					if (tipo.equalsIgnoreCase("operazione")) {
						operazione(column, tabella);
					} else if (tipo.equalsIgnoreCase("input")) {
						inputM(column, tabella);
					} else {
						System.out.println("Modifica inserita non valida");
					}
				}
				default -> {
					System.out.println("Modifica possibile: input");
					inputM(column, tabella);
				}
			}
			System.out.println("Effettuare altre modifiche (modify) alla colonna: " +column.getName()+"? (si/no)");
			risp = risposta();			
		}
		System.out.println("Fine modify.");
	}
	
	/*Metodo per la modifica: OPERAZIONE
	 * - chiede all'utente l'operazione aritmetica da eseguire
	 * - chiede all'utente condizione, che permette di scegliere
	 *   i record su cui effettuare l'operazione aritmetica
	 * - effettua l'operazione
	 */ 
	public static void operazione(Column colonna, String tabella) {
		String operazione = "";
		System.out.println("Inserisce l'operazione aritmetica. (es + 10)");
		operazione = tastiera.nextLine();
		System.out.println("Inserisci condizione (esempio !=1)");
		cond = tastiera.nextLine();
		try {
			query="UPDATE "+tabella+ " SET "+colonna.getName()+"="+colonna.getName()+operazione+" " +
					"WHERE "+colonna.getName()+cond;
			st = DBConnection.getConn().createStatement();
			st.executeUpdate(query);
			System.out.println("Operazione "+operazione+" eseguita");
		}catch(SQLException se) {
			System.out.println("Error: " +se.getMessage());
		}finally {
			DBConnection.closeSt(st);
		}
	}
	
	/*Metodo per la modifica: INPUT
	 * - chiede all'utente il nuovo valore sostitutivo
	 * - se la colonna � di tipo numerico, chiede all'utente di inserire la condizione 
	 *   che permette di scegliere i record da modificare
	 * - altrimenti chiede esplicitamente all'utente il valore che vuole modificare
	 * - effettua la sostituzione con il nuovo valore inserito in input
	 */ 
	public static void inputM(Column column, String tabella) {
		System.out.println("Inserisci nuovo valore");
		val = tastiera.nextLine();
		switch (column.getDatatype()) {
			case Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.DECIMAL,
					Types.FLOAT, Types.REAL, Types.TINYINT -> {
				cond = tastiera.nextLine();
				try {
					query = "UPDATE "+tabella+ " SET "+column.getName()+ "='"+val+"' WHERE "+column.getName()+cond;
					st = DBConnection.getConn().createStatement();
					st.executeUpdate(query);
					System.out.println("Update eseguito, inserito nuovo valore: "+val);
				}catch(SQLException se) {
					System.out.println("Error: "+se.getMessage());
				}finally {
					DBConnection.closeSt(st);
				}
			}
			default -> {
				System.out.println("Inserisci il valore da modificare");
				String vm = tastiera.nextLine();
				try {
					query = "UPDATE "+tabella+" SET "+column.getName()+"= '"+val+"' WHERE "+column.getName()+"='"+vm+"'";
					st = DBConnection.getConn().createStatement();
					st.executeUpdate(query);
					System.out.println("Update eseguito, inserito nuovo valore: "+val);
				}catch(SQLException se) {
					System.out.println("Error: "+se.getMessage());
				}finally {
					DBConnection.closeSt(st);
				}
			}
		}
	}
	//Metodo che restituisce la risposta dell'utente (si/no)
	public static String risposta() {
		boolean run=true;
		String input ="";
		while(run) {
			input = tastiera.nextLine();
			if(input.equalsIgnoreCase("si")||input.equalsIgnoreCase("no")) {
				run=false;
			}else {
				System.out.println("Attenzione, inserire si o no.");
			}
		}
		return input;
	}

	
	//to check table existence
	public static boolean searchTable(String table, String db) {
		try {
			DatabaseMetaData dbm = DBConnection.getConn().getMetaData();
			rs = dbm.getTables(db, null, table, null);
			if(!rs.next()) {
				System.out.println("Table not found.");
				return false;
			}
			DBConnection.closeRs(rs);
			return true;
		}catch(SQLException se){
			System.out.println("Error: "+se.getMessage());
			DBConnection.closeRs(rs);
			return false;
		}
	}

	/**
	 * Genera un valore randomico per un dato field che non sia una foreignKey.
	 * Se il field � una chiave primaria non esterna che non si autoincrementa,
	 * il metodo valuateKey deve essere usato per controllare
	 * l'unicit� della combinazione di chiavi quando tutte sono state generate.
	 * @param column colonna di cui generare il valore
	 * @return Valore generato
	 * @throws Exception
	 */
	private static String generateField(Column column) throws Exception{
		//TODO words
		String[] words = {"a", "b", "c", "Palla"};
		Random r = new Random();
		String generatedValue = "";
		//TODO add for remaining types if useful
		switch (column.getDatatype()) {
			case Types.INTEGER -> {
				int random = r.nextInt(305);
				generatedValue = random + "";

			}
			case Types.BIGINT -> {
				long random = r.nextLong();
				generatedValue = random + "";
			}
			case Types.DATE -> {

				Date random = new Date(r.nextLong());
				generatedValue = "CONVERT('" +random.toString() + "', DATE)";
			}
			case Types.FLOAT, Types.REAL -> {
				float random = r.nextFloat();
				generatedValue = random + "";
			}
			case Types.DOUBLE -> {
				double random = r.nextDouble();
				generatedValue = random + "";
			}
			case Types.TIME -> {
				Time random = new Time(r.nextLong());
				generatedValue = "CONVERT('" +random.toString() + "', TIME)";
			}

			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR -> {
				String random = "";
				do {
					random = words[r.nextInt(words.length)];
				} while(random.length() > column.getColumnSize());

				generatedValue = "'" + random + "'";
			}
		}


		return generatedValue;
	}

	/** Genera una chiave esterna randomicamente dalla tabella referenziata
	 *
	 * @param column colonna ForeignKey
	 * @return Valore scelto a caso
	 * @throws Exception Ogni SQLException
	 */
	private static String generateForeignKey(Column column) throws Exception{
		ForeignKeyColumn fk = (ForeignKeyColumn) column;
		String generatedValue = "";
		//Prende un valore a caso della Fk tra i valori possibili
		String query = "SELECT " + fk.getReferredPrimaryKey() + " " +
				"FROM " + fk.getReferredTable() + " " +
				"ORDER BY RAND() LIMIT 1";
		Statement st = DBConnection.getConn().createStatement();
		ResultSet rs = st.executeQuery(query);
		rs.next();
		switch (column.getDatatype()){
			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR  -> {
				//values.add(columns.indexOf(c), "'rock'");
				generatedValue = "'" + rs.getString(fk.getReferredPrimaryKey()) + "'";
				//values.add(columns.indexOf(column), "'" + rs.getString(fk.getReferencedPrimaryKey()) + "'");
			}
			default -> {
				//TODO check if this is fine for each type
				generatedValue = rs.getObject(fk.getReferredPrimaryKey()) + "";
				//values.add(columns.indexOf(column), rs.getObject(fk.getReferencedPrimaryKey()) + "");
			}
		}
		st.close();
		return generatedValue;
	}

	/** Effettua una query che controlla se i valori generati delle chiavi primarie sono gi� presenti
	 *
	 * @param values Valori delle chiavi primarie
	 * @param table	 Tabella
	 * @return	True se la combinazione di chiavi non � presente nella tabella
	 * @throws Exception Ogni SQLException
	 */
	private static boolean valuateKeys(List<String> values, String table) throws Exception{
		boolean onlyIncrementKey = false;
		List<Column> columns = MainDebug.graph.getColumnsInTable(table);
		List<Column> primaryKeys = MainDebug.graph.getPrimaryKeysInTable(table);
		String primaryKeysQuery = "", whereConditions = "";
		for (int i = 0; i < primaryKeys.size(); ++i) {
			//Se il valore dato alla primaryKey � AUTO_INCREMENT allora la colonna non deve essere considerata
			//Nella query. Se quella � l'unica colonna
			if (primaryKeys.get(i).isAutoIncrementing()) {
				//TODO se tutte le colonne sono autoincrement ma non so se � un caso possibile
				if (primaryKeys.size() == 1) onlyIncrementKey = true;
			} else {
				//Altrimenti costruisce la where clause per fare la query
				whereConditions += primaryKeys.get(i).getName() + "=" + values.get(columns.indexOf(primaryKeys.get(i))) + " and ";
			}
		}
		if (!onlyIncrementKey) {
			//Remove the last and
			whereConditions = whereConditions.substring(0, whereConditions.length() - 5);

			//Completo ed eseguo la query
			primaryKeysQuery += whereConditions;
			ResultSet check = DBConnection.getConn().createStatement().executeQuery(primaryKeysQuery);

			return check.next();
		}
		return onlyIncrementKey;
	}

	
	
	
	
}




