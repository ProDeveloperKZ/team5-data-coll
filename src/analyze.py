import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import sqlite3
from matplotlib.colors import BoundaryNorm
from matplotlib.colors import PowerNorm

def prepare_data_frame(df):
    dates = pd.to_datetime(df["time"], unit = "ms", utc = True)
    df["year-month"] = dates.dt.strftime("%Y-%m")
    rows = np.floor((df["latitude"]+90)/30).astype(int)
    rows = rows.clip(upper = 5)
    longitude = (df["longitude"]+180)%360 - 180
    cols = np.floor((longitude + 180)/60).astype(int)
    df["sector_id"] = "R" + rows.astype(str) + "_C" + cols.astype(str)
    df["month"] = dates.dt.month_name()
    df["year"] = dates.dt.year
    seasons = {
        "2023-01" : "Winter 2023",
        "2023-02" : "Winter 2023",
        "2023-03" : "Spring 2023",
        "2023-04" : "Spring 2023",
        "2023-05" : "Spring 2023",
        "2023-06" : "Summer 2023", 
        "2023-07" : "Summer 2023",
        "2023-08" : "Summer 2023",
        "2023-09" : "Fall 2023",
        "2023-10" : "Fall 2023",
        "2023-11" : "Fall 2023",
        "2023-12" : "Winter 2024",
        "2024-01" : "Winter 2024",
        "2024-02" : "Winter 2024",
        "2024-03" : "Spring 2024",
        "2024-04" : "Spring 2024",
        "2024-05" : "Spring 2024",
        "2024-06" : "Summer 2024", 
        "2024-07" : "Summer 2024",
        "2024-08" : "Summer 2024",
        "2024-09" : "Fall 2024",
        "2024-10" : "Fall 2024",
        "2024-11" : "Fall 2024",
        "2024-12" : "Winter 2025",
        "2025-01" : "Winter 2025",
        "2025-02" : "Winter 2025",
        "2025-03" : "Spring 2025",
        "2025-04" : "Spring 2025",
        "2025-05" : "Spring 2025",
        "2025-06" : "Summer 2025", 
        "2025-07" : "Summer 2025",
        "2025-08" : "Summer 2025",
        "2025-09" : "Fall 2025",
        "2025-10" : "Fall 2025",
        "2025-11" : "Fall 2025",
        "2025-12" : "Winter 2026",
        "2026-01" : "Winter 2026",
        "2026-02" : "Winter 2026",
        "2026-03" : "Spring 2026",
        "2026-04" : "Spring 2026",
        "2026-05" : "Spring 2026",
        "2026-06" : "Summer 2026", 
        "2026-07" : "Summer 2026",
        "2026-08" : "Summer 2026",
        "2026-09" : "Fall 2026",
        "2026-10" : "Fall 2026",
        "2026-11" : "Fall 2026"
    }
    season_no_year = {
            'January' : 'Winter',
            'February' : 'Winter',
            'March' : 'Spring',
            'April' : 'Spring',
            'May' : 'Spring',
            'June' : 'Summer',
            'July' : 'Summer',
            'August' : 'Summer',
            'September' : 'Fall',
            'October' : 'Fall',
            'November' : 'Fall',
            'December' : 'Winter'
        }

    df["season"] = df["year-month"].map(seasons)

    df['season(no_year)'] = df['month'].map(season_no_year)

    df["quakes_per_month"] = df.groupby("year-month")["id"].transform("size")

    df["quakes_per_season"] = df.groupby("season")["id"].transform("size")

    df["quakes_per_sector"] = df.groupby("sector_id")["id"].transform("size")

    df["quakes_per_sector_month"] = (
        df.groupby(["sector_id", "year-month"])["id"].transform("size")
    )

    df['cols'] = cols
    df['rows'] = rows
    return df

def view_map_of_quakes(df):
    dottsize = {
        4.5 : 0.5,
        4.53 : 0.5,
        4.54 : 0.5,
        4.56 : 0.6,
        4.57 : 0.6,
        4.58 : 0.7,
        4.6 : 0.7,
        4.61 : 0.7,
        4.65 : 0.7,
        4.7 : 0.8,
        4.78 : 0.8,
        4.8 : 1,
        4.81 : 1,
        4.83 : 1,
        4.86 : 1,
        4.9 : 1,
        4.92 : 1,
        4.99 :2,
        5.0 : 2,
        5.06 : 2,
        5.08 : 2,
        5.1 : 2,
        5.16 : 2,
        5.2 : 2,
        5.25 : 2,
        5.3 : 3,
        5.35 : 3,
        5.4 : 3,
        5.48 : 3,
        5.5 : 3,
        5.58 : 3,
        5.6 : 3,
        5.7 : 4,
        5.8 : 4,
        5.88 : 4,
        5.9 : 4,
        6.0 : 5,
        6.1 : 6,
        6.2 : 7,
        6.3 : 8,
        6.4 : 9,
        6.5 : 10,
        6.6 : 11,
        6.7 : 12,
        6.8 : 13,
        6.9 : 14,
        7.0 : 15,
        7.1 : 17,
        7.2 : 19,
        7.3 : 21,
        7.4 : 23,
        7.5 : 25,
        7.6 : 27,
        7.7 : 29,
        7.8 : 31,
        7.9 : 33,
        8.0 : 35,
        8.1 : 38,
        8.2 : 41,
        8.3 : 45,
        8.4 : 48,
        8.5 : 51,
        8.6 : 54,
        8.7 : 57,
        8.8 : 60
        }

    boundaries = [4.5, 4.6, 4.7, 4.8, 4.9,5.0, 5.5, 6, 7, 8, 9]
    ylocs = [-90, -60, -30, 0, 30, 60, 90]
    xlocs = [-180, -120, -60, 0, 60, 120, 180]
    dotsize = df["mag"].map(dottsize)
    bn = BoundaryNorm(boundaries=boundaries, ncolors=256)

    plt.figure(figsize=(14, 6))
    ax = plt.axes(projection=ccrs.Robinson())
    ax.coastlines(resolution='110m')
    ax.set_global()

    gl = ax.gridlines(draw_labels=True, xlocs = xlocs, ylocs = ylocs)
    gl.top_labels = False
    gl.right_labels = False

    sc = ax.scatter(x = df["longitude"], y = df["latitude"], s =dotsize, transform=ccrs.PlateCarree(), c=df["mag"], cmap='coolwarm', norm=bn)
    plt.colorbar(mappable=sc, orientation = 'horizontal', spacing = 'uniform', ticks = boundaries, label = 'Magnitude', shrink = 0.6 )
    plt.title('Recorded Earthquakes on Map(01.01.23 - 18.11.26)')
    plt.savefig("figures/MapofQuakes.png", dpi=300, bbox_inches="tight")
    plt.show()

def view_activity_by_sec_map(df):
    ylocs = [-90, -60, -30, 0, 30, 60, 90]
    xlocs = [-180, -120, -60, 0, 60, 120, 180]
    pn = PowerNorm(gamma=0.5)
    bcm = [0, 100, 250, 500, 1000, 2000, 4000, 5000]
    sect_mat = pd.crosstab(df['rows'], df['cols'])

    plt.figure(figsize=(14, 6))
    ax1 = plt.axes(projection=ccrs.Robinson())
    ax1.coastlines(resolution='110m')
    ax1.set_global()
    gl = ax1.gridlines(draw_labels=True, xlocs = xlocs, ylocs = ylocs)
    gl.top_labels = False
    gl.right_labels = False
    cm =ax1.pcolormesh(xlocs, ylocs, sect_mat, transform=ccrs.PlateCarree(), cmap="YlOrRd", norm=pn)
    plt.colorbar(mappable=cm, orientation = 'horizontal', spacing = 'uniform', ticks = bcm, label = 'Number of Earthquakes', shrink = 0.6 )
    plt.title('Activity by Sector(01.01.23 - 18.11.26)')
    plt.savefig("figures/Sectormap.png", dpi=300, bbox_inches="tight")
    plt.show()

def view_quakes_per_month(df):
    plt.figure(figsize= (14, 6))
    plt.plot(df["year-month"],df['quakes_per_month'], )
    plt.title("Earthquakes per Month")
    plt.xlabel("Year-Month")
    plt.ylabel("Number of Earthquakes")
    plt.xticks(rotation = 90, fontsize = 8)
    plt.savefig("figures/Quakespermonth.png", dpi=300, bbox_inches="tight")
    plt.show()

def view_quakes_per_season(df):
    plt.figure(figsize= (14, 6))
    plt.plot(df["season"],df['quakes_per_season'], )
    plt.title("Earthquakes per Season")
    plt.xlabel("Season")
    plt.ylabel("Number of Earthquakes")
    plt.xticks(rotation = 90, fontsize = 6)
    plt.savefig("figures/Quakesperseason.png", dpi=300, bbox_inches="tight")
    plt.show()

def view_most_active_10(df):
    sect_mat = pd.crosstab(df["rows"], df["cols"])
    sect_mat = sect_mat.reset_index()
    secbar = sect_mat.melt(id_vars='rows', var_name= 'cols', value_name= 'count')
    secbar["sector_id"] = 'R' + secbar['rows'].astype(str) + '_C' + secbar['cols'].astype(str)
    secbar=secbar.drop(columns=['rows', 'cols'])

    secbar2 = secbar.sort_values(by="count", ascending=False).head(10)
    plt.figure(figsize= (14, 6))
    plt.bar(secbar2["sector_id"],secbar2['count'])
    plt.title("Most Active Sectors")
    plt.xlabel("Sector Id")
    plt.ylabel("Number of Earthquakes")
    plt.xticks(rotation = 90, fontsize = 6)
    plt.savefig("figures/top10.png", dpi=300, bbox_inches="tight")
    plt.show()

def view_least_active_10(df):
    sect_mat = pd.crosstab(df["rows"], df["cols"])
    sect_mat = sect_mat.reset_index()
    secbar = sect_mat.melt(id_vars='rows', var_name= 'cols', value_name= 'count')
    secbar["sector_id"] = 'R' + secbar['rows'].astype(str) + '_C' + secbar['cols'].astype(str)
    secbar=secbar.drop(columns=['rows', 'cols'])

    secbar1 = secbar.sort_values(by="count", ascending=False).tail(10)
    plt.figure(figsize= (14, 6))
    plt.bar(secbar1["sector_id"],secbar1['count'])
    plt.title("Least Active Sectors")
    plt.xlabel("Sector Id")
    plt.ylabel("Number of Earthquakes")
    plt.xticks(rotation = 90, fontsize = 6)
    plt.savefig("figures/least10.png", dpi=300, bbox_inches="tight")
    plt.show()

def view_dist_mag(df):
    mag_per_sector = df[['mag', 'sector_id']]
    mag_bins = [4.5, 5, 6, 7, 8, 9]

    plt.figure(figsize=(14, 6))
    patches = plt.hist(mag_per_sector['mag'], bins=mag_bins,linewidth=0.8, color="steelblue", edgecolor='black', rwidth=0.9)[2]
    plt.bar_label(patches)
    plt.yscale('symlog', linthresh=1)
    plt.title("Distribution of Recorded Earthquakes by Magnitude")
    plt.xlabel('Magnitude')
    plt.ylabel('Number of Records(logarithmic)')
    plt.savefig("figures/distribution.png", dpi=300, bbox_inches="tight")
    plt.show()

def database_review(df):
    print('Database contains',df.shape[0],'records(rows) and',df.shape[1],'columns.')
    print("\nStructure of database:")
    df.info()
    print(df.describe())
    print(df.describe(include="string"))

def mean_by_month(df):
    month_order = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December']
    df['month'] =  pd.Categorical(df['month'], categories=month_order, ordered=True)
    info_by_month = df.groupby(['year-month'])['mag'].agg(['mean', 'max', 'min', 'median']).reset_index()
    plt.figure(figsize= (14, 6))
    plt.plot(info_by_month["year-month"],info_by_month['mean'], label = 'Mean Value')
    plt.xticks(rotation = 90, fontsize = 8)
    plt.title("Mean Magnitude by Month")
    plt.ylabel('Magnitude')
    plt.xlabel('Month')
    plt.plot(info_by_month['year-month'],info_by_month['max'], ls='--', label = 'Maximal value')
    plt.plot(info_by_month['year-month'],info_by_month['median'], ls=':', label = 'Median value')
    plt.legend()
    plt.savefig("figures/meanbymonth.png", dpi=300, bbox_inches="tight")
    plt.show()

def main():
    con = sqlite3.connect("output.db")
    df = pd.read_sql_query("SELECT * FROM Records", con)
    df = prepare_data_frame(df)
    view_dist_mag(df)
    view_map_of_quakes(df)
    view_activity_by_sec_map(df)
    view_least_active_10(df)
    view_most_active_10(df)
    view_quakes_per_month(df)
    view_quakes_per_season(df)
    database_review(df)
    mean_by_month(df)
    con.close()

if __name__ == "__main__":
    main()