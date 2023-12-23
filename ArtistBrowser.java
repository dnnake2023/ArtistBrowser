//THIS CODE IS MY OWN WORK, IT WAS WRITTEN WITHOUT CONSULTING
//A TUTOR OR CODE WRITTEN BY OTHER STUDENTS - Dubem Nnake
import java.util.ArrayList;
import java.sql.*;
import java.util.Collections;
import java.util.HashSet;

public class ArtistBrowser {

	/* A connection to the database */
	private Connection connection;

	/**
	 * Constructor loads the JDBC driver. No need to modify this.
	 */
	public ArtistBrowser() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("Failed to locate the JDBC driver.");
		}
	}

	/**
	 * Establishes a connection to be used for this session, assigning it to the
	 * private instance variable 'connection'.
	 *
	 * @param url      the url to the database
	 * @param username the username to connect to the database
	 * @param password the password to connect to the database
	 * @return true if the connection is successful, false otherwise
	 */
	public boolean connectDB(String url, String username, String password) {
		try {
			this.connection = DriverManager.getConnection(url, username, password);
			return true;
		} catch (SQLException se) {
			System.err.println("SQL Exception: " + se.getMessage());
			return false;
		}
	}

	/**
	 * Closes the database connection.
	 *
	 * @return true if the closing was successful, false otherwise.
	 */
	public boolean disconnectDB() {
		try {
			this.connection.close();
			return true;
		} catch (SQLException se) {
			System.err.println("SQL Exception: " + se.getMessage());
			return false;
		}
	}

	/**
	 * Returns a sorted list of the names of all musicians who were part of a band
	 * at some point between a given start year and an end year (inclusive).
	 *
	 * Returns an empty list if no musicians match, or if the given timeframe is
	 * invalid.
	 *
	 * NOTE: Use Collections.sort() to sort the names in ascending alphabetical
	 * order. Use prepared statements.
	 *
	 * @param startYear
	 * @param endYear
	 * @return a sorted list of artist names
	 */
	public ArrayList<String> findArtistsInBands(int startYear, int endYear) {

		ArrayList<String> artists = new ArrayList<>();

		//startyear >= startYear  AND startyear <= endyear
		String SQL = "SELECT DISTINCT name from artist " + " JOIN wasinband on artist.artist_id = wasinband.artist_id "
				+ " JOIN role ON artist.artist_id = role.artist_id"
				+ " WHERE end_year >= ? AND start_year <= ? AND role.role IN ('Musician')";

		try {
			// create prepared statements
			PreparedStatement preparedStatement = connection.prepareStatement(SQL);
			preparedStatement.setInt(1, startYear);
			preparedStatement.setInt(2, endYear);


			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				artists.add(rs.getString("name"));
			}

			// sort the collection
			Collections.sort(artists);

		} catch (SQLException ex) {
			// System.out.println(ex.getMessage());
			ex.printStackTrace();
		}

		return artists;
	}

	/**
	 * Returns a sorted list of the names of all musicians and bands who released at
	 * least one album in a given genre.
	 *
	 * Returns an empty list if no such genre exists or no artist matches.
	 *
	 * NOTE: Use Collections.sort() to sort the names in ascending alphabetical
	 * order. Use prepared statements.
	 *
	 * @param genre the genre to find artists for
	 * @return a sorted list of artist names
	 */
	public ArrayList<String> findArtistsInGenre(String genre) {

		ArrayList<String> artists = new ArrayList<>();
		String SQL = "SELECT DISTINCT name from artist " + " JOIN album on artist.artist_id = album.artist_id  "
				+ " JOIN genre ON album.genre_id = genre.genre_id " + " JOIN role  ON artist.artist_id = role.artist_id"
				+ " WHERE genre =?  AND role.role IN ('Band','Musician')";

		try {
			// create prepared statements
			PreparedStatement preparedStatement = connection.prepareStatement(SQL);
			preparedStatement.setString(1, genre);
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				artists.add(rs.getString("name"));
			}

			// sort the collection
			Collections.sort(artists);

		} catch (SQLException ex) {
			// System.out.println(ex.getMessage());
			ex.printStackTrace();
		}

		return artists;
	}

	/**
	 * Returns a sorted list of the names of all collaborators (either as a main
	 * artist or guest) for a given artist.
	 *
	 * Returns an empty list if no such artist exists or the artist has no
	 * collaborators.
	 *
	 * NOTE: Use Collections.sort() to sort the names in ascending alphabetical
	 * order. Use prepared statements.
	 *
	 * @param artist the name of the artist to find collaborators for
	 * @return a sorted list of artist names
	 */
	public ArrayList<String> findCollaborators(String artist) {
		ArrayList<String> artists = new ArrayList<>();

		// first get all guests of this artist
		String guests = "SELECT artist2,name FROM collaboration  JOIN artist  "
				+ " ON artist.artist_id = collaboration.artist2 "
				+ " WHERE artist1 IN (SELECT artist_id FROM artist WHERE name =?)";

		// get artists names where this artist is a guest
		String artistss = "SELECT artist1,name FROM collaboration  JOIN artist  "
				+ " ON artist.artist_id = collaboration.artist1 "
				+ " WHERE artist2 IN (SELECT artist_id FROM artist WHERE name =?)";

		try {
			// create prepared statements
			PreparedStatement guestsStatement = connection.prepareStatement(guests);
			guestsStatement.setString(1, artist);
			ResultSet rs = guestsStatement.executeQuery();

			while (rs.next()) {
				artists.add(rs.getString("name"));
			}

			// now get the artists
			PreparedStatement artistsStatement = connection.prepareStatement(artistss);
			artistsStatement.setString(1, artist);
			ResultSet rs2 = artistsStatement.executeQuery();
			while (rs2.next()) {
				artists.add(rs2.getString("name"));
			}

			// sort the collection
			Collections.sort(artists);

		} catch (SQLException ex) {
			// System.out.println(ex.getMessage());
			ex.printStackTrace();
		}

		return artists;
	}

	/**
	 * Returns a sorted list of the names of all songwriters who wrote songs for a
	 * given artist (the given artist is excluded).
	 *
	 * Returns an empty list if no such artist exists or the artist has no other
	 * songwriters other than themself.
	 *
	 * NOTE: Use Collections.sort() to sort the names in ascending alphabetical
	 * order.
	 *
	 * @param artist the name of the artist to find the songwriters for
	 * @return a sorted list of songwriter names
	 */
	public ArrayList<String> findSongwriters(String artist) {
		ArrayList<String> songwriters = new ArrayList<>();
		int artistID = 0;
		// get the artist id, to simplify the query
		String artID = "SELECT artist_id FROM artist WHERE name =?";

		// get song writers
		String songWritersSQL = "SELECT DISTINCT name from artist "
				+ " JOIN song on artist.artist_id = song.songwriter_id "
				+ " JOIN belongstoalbum  ON song.song_id = belongstoalbum.song_id"
				+ " JOIN album  ON album.album_id = belongstoalbum.album_id"
				+ " JOIN role  ON artist.artist_id = role.artist_id"
				+ " WHERE album.artist_id = ?  AND role.role IN ('Songwriter')";

		try {
			// create prepared statements
			PreparedStatement artsStmt = connection.prepareStatement(artID);
			artsStmt.setString(1, artist);
			ResultSet rsArtist = artsStmt.executeQuery();

			while (rsArtist.next()) {
				artistID = rsArtist.getInt("artist_id");
			}

			// songWritersSQL
			PreparedStatement sngWriterStmt = connection.prepareStatement(songWritersSQL);
			sngWriterStmt.setInt(1, artistID);
			ResultSet rsSongWriters = sngWriterStmt.executeQuery();

			while (rsSongWriters.next()) {
				String art = rsSongWriters.getString("name");
				// exclude own self
				if (!artist.equals(art)) {
					songwriters.add(art);
				}

			}

			// sort the collection
			Collections.sort(songwriters);

		} catch (SQLException ex) {
			// System.out.println(ex.getMessage());
			ex.printStackTrace();
		}

		return songwriters;
	}

	/**
	 * Returns a sorted list of the names of all common acquaintances for a given
	 * pair of artists.
	 *
	 * Returns an empty list if either of the artists does not exist, or they have
	 * no acquaintances.
	 *
	 * NOTE: Use Collections.sort() to sort the names in ascending alphabetical
	 * order.
	 *
	 * @param artist1 the name of the first artist to find acquaintances for
	 * @param artist2 the name of the second artist to find acquaintances for
	 * @return a sorted list of artist names
	 */
	public ArrayList<String> findCommonAcquaintances(String artist1, String artist2) {
		// Song writers
		ArrayList<String> sngWriters1 = findSongwriters(artist1);
		// collaborators of 1
		ArrayList<String> collaborators1 = findCollaborators(artist1);

		ArrayList<String> sngWriters2 = findSongwriters(artist2);
		// collaborators of 1
		ArrayList<String> collaborators2 = findCollaborators(artist2);
		// combine all the 4 lists
		ArrayList<String> newList = new ArrayList<String>();
		newList.addAll(sngWriters1);
		newList.addAll(collaborators1);
		newList.addAll(sngWriters2);
		newList.addAll(collaborators2);

		// eliminate any duplicates by putting the list in a HashSet
		// HashSet doesn't take any duplicate value
		return new ArrayList<String>(new HashSet<>(newList));
	}

	/**
	 * Returns true if two artists have a collaboration path connecting them in the
	 * database (see A3 handout for our definition of a path). For example, artists
	 * `Z' and `Usher' are considered connected even though they have not
	 * collaborated directly on any song, because 'Z' collaborated with `Alicia
	 * Keys' who in turn had collaborated with `Usher', therefore there is a
	 * collaboration path connecting `Z' and `Usher'.
	 *
	 * Returns false if there is no collaboration path at all between artist1 and
	 * artist2 or if either of them do not exist in the database.
	 *
	 * @return true iff artist1 and artist2 have a collaboration path connecting
	 *         them
	 */
	public boolean artistConnectivity(String artist1, String artist2) {
		// collaborators of 1
		ArrayList<String> collaborators1 = findCollaborators(artist1);

		//Level 1 is to check if artist2 is a direct collaborator
		for(String s: collaborators1 ) {
			if(s.equals(artist2)) {
				return true;
			}
		}
		//if the above loop did not find a link, we go deeper
		for(String s: collaborators1 ) {
			ArrayList<String> innerCollabo = findCollaborators(s);

			for(String s2: innerCollabo ) {
				if(s2.equals(artist2)) {
					return true;
				}
			}
		}
		return false; // return false if no connection was found
	}




	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: java ArtistBrowser <userName> <password>");
			return;
		}

		String user = args[0];
		String pass = args[1];

		ArtistBrowser a3 = new ArtistBrowser();

		String url = "jdbc:postgresql://localhost:5432/postgres?currentSchema=artistDB";
		a3.connectDB(url, user, pass);

		System.err.println("\n----- ArtistsInBands -----");
		ArrayList<String> res = a3.findArtistsInBands(1990, 1999);
		for (String s : res) {
			System.err.println(s);
		}

		 System.err.println("\n----- ArtistsInGenre -----");
		 res = a3.findArtistsInGenre("Rock");
		 for (String s : res) {
		 	System.err.println(s);
		 }

		 System.err.println("\n----- Collaborators -----");
		 res = a3.findCollaborators("Usher");
		 for (String s : res) {
		 	System.err.println(s);
		}

		 System.err.println("\n----- Songwriters -----");
		 res = a3.findSongwriters("Justin Bieber");
		 for (String s : res) {
			System.err.println(s);
		 }

		 System.err.println("\n----- Common Acquaintances -----");
		 res = a3.findCommonAcquaintances("Jaden Smith", "Miley Cyrus");
		 for (String s : res) {
		 	System.err.println(s);
		 }

		 System.err.println("\n----- artistConnectivity -----");
		 String a1 = "Z", a2 = "Usher";
		 boolean areConnected = a3.artistConnectivity(a1, a2);
		 System.err.println("Do artists " + a1 + " and " + a2 + " have a collaboration path connecting them? Answer: "
		 		+ areConnected);

		a3.disconnectDB();
	}
}
