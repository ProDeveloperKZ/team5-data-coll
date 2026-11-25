import sqlite3
import pandas as pd

def create_table(con):
    with con:
        con.execute("CREATE TABLE IF NOT EXISTS Records"
                "(id TEXT PRIMARY KEY, mag REAL, magType TEXT, net TEXT, place TEXT, time TEXT, updated TEXT, " \
                "type TEXT, status TEXT, tsunami INTEGER, sig INTEGER, felt REAL, alert TEXT, nst REAL, gap REAL," \
                " dmin REAL, longitude REAL, latitude REAL, depth_km REAL, region TEXT, energy_joules REAL, log_energy REAL, " \
                "depth_class TEXT, year INTEGER)")

def insert_data(con, data):
    with con:
        data["tsunami"] = data["tsunami"].astype(int)
        data["nst"] = data["nst"].astype(object).where(data["nst"].notna(), None)
        data["gap"] = data["gap"].astype(object).where(data["gap"].notna(), None)
        data["dmin"] = data["dmin"].astype(object).where(data["dmin"].notna(), None)
        datainlist = list(data.itertuples(name=None,index=False))
        con.executemany("INSERT OR IGNORE INTO Records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", datainlist)

def get_number_of_rows(con):
    with con:
        return con.execute("SELECT COUNT(id) FROM Records").fetchone()[0]

def get_structure_of_table(con):
    with con:
        return con.execute("PRAGMA table_info(Records)").fetchall()

def get_empty_id(con):
    with con:
        return con.execute("SELECT COUNT(*) FROM Records WHERE id IS NULL").fetchone()[0]

def get_empty_mag(con):
    with con:
        return con.execute("SELECT COUNT(*) FROM Records WHERE mag IS NULL").fetchone()[0]

def get_empty_time(con):
    with con:
        return con.execute("SELECT COUNT(*) FROM Records WHERE time IS NULL").fetchone()[0]

def get_maximal_mag(con):
    with con:
        return con.execute("SELECT MAX(mag) FROM Records").fetchone()[0]

def get_minimal_mag(con):
    with con:
        return con.execute("SELECT MIN(mag) FROM Records").fetchone()[0]

def get_average_mag(con):
    with con:
        return con.execute("SELECT AVG(mag) FROM Records").fetchone()[0]

def get_maximal_depth(con):
    with con:
        return con.execute("SELECT MAX(depth_km) FROM Records").fetchone()[0]

def get_minimal_depth(con):
    with con:
        return con.execute("SELECT MIN(depth_km) FROM Records").fetchone()[0]

def get_average_depth(con):
    with con:
        return con.execute("SELECT AVG(depth_km) FROM Records").fetchone()[0]

def get_first_record_time(con):
    with con:
        return con.execute("SELECT MIN(time) FROM Records").fetchone()[0]

def get_last_record_time(con):
    with con:
        return con.execute("SELECT MAX(time) FROM Records").fetchone()[0]


def main():
    con = sqlite3.connect("output.db")
    df = pd.read_csv("data/cleaned/cleaned_earthquakes.csv")
    try:
        create_table(con)
        insert_data(con, df)
    
    #CHECKING
        print("Number of rows in initial dataframe(csv) -", df["id"].count())
        print("Number of rows in DataBase -", get_number_of_rows(con))
        insert_data(con,df)
        print("SECOND INSERT")
        print("Number of rows in initial dataframe(csv) after second insert -", df["id"].count())
        print("Number of rows in DataBase after second insert -", get_number_of_rows(con))

        print("Info about Table - ", get_structure_of_table(con))

        print("Number of Empty ID`s in database -",get_empty_id(con))

        print("Number of Empty magnitudes in database", get_empty_mag(con))

        print("Number of Empty record times in database", get_empty_time(con))

        print("Maximal magnitude in database -", get_maximal_mag(con), ", and in initial dataframe(csv)", df["mag"].max())
        print("Minimal magnitude in database -", get_minimal_mag(con), ", and in initial dataframe(csv)", df["mag"].min())
        print("Average magnitude in database -", get_average_mag(con), ", and in initial dataframe(csv)", df["mag"].mean())

        print("Maximal depth in database -", get_maximal_depth(con), ", and in initial dataframe(csv)", df["depth_km"].max())
        print("Minimal depth in database -", get_minimal_depth(con), ", and in initial dataframe(csv)", df["depth_km"].min())
        print("Average depth in database -", get_average_depth(con), ", and in initial dataframe(csv)", df["depth_km"].mean())

        print("First recording time in database -", get_first_record_time(con),", and in initial dataframe(csv)", df["time"].min())
        print("Last recording time in database -", get_last_record_time(con),", and in initial dataframe(csv)", df["time"].max())

        
    finally:
        con.close()
   
if __name__ == "__main__":
    main()