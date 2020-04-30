import csv
import psycopg2

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

conn = psycopg2.connect(connection_string)

cursor = conn.cursor()

"""
Takes the initals of a team and returns its associated name
"""
def init_to_name(initial):
    if initial is 'BUF':
        return 'Buffalo Bills'
    if initial is 'MIA':
        return 'Miami Dolphins'
    if initial is 'NE':
        return 'New England Patriots'
    if initial is 'NYJ':
        return 'New York Giants'
    if initial is 'BAL':
        return 'Baltimore Ravens'
    if initial is 'CIN':
        return 'Cincinnati Bengals'
    if initial is 'CLE':
        return 'Cleveland Browns'
    if initial is 'PIT':
        return 'Pittsburgh Steelers'
    if initial is 'HOU':
        return 'Houston Texans'
    if initial is 'IND':
        return 'Indianapolis Colts'
    if initial is 'JAX':
        return 'Jacksonville Jaguars'
    if initial is 'TEN':
        return 'Tennessee Titans'
    if initial is 'DEN':
        return 'Denver Broncos'
    if initial is 'KC':
        return 'Kansas City Chiefs'
    if initial is 'OAK':
        return 'Oakland Raiders'
    if initial is 'SD':
        return 'San Diego Chargers'
    if initial is 'DAL':
        return 'Dallas Cowboys'
    if initial is 'NYG':
        return 'New York Giants'
    if initial is 'PHI':
        return 'Philadelphia Eagles'
    if initial is 'WAS':
        return 'Washington Redskins'
    if initial is 'CHI':
        return 'Chicago Bears'
    if initial is 'DET':
        return 'Detroit Lions'
    if initial is 'GB':
        return 'Green Bay Packers'
    if initial is 'MIN':
        return 'Minnesota Vikings'
    if initial is 'ATL':
        return 'Atlanta Falcons'
    if initial is 'CAR':
        return 'Carolina Panthers'
    if initial is 'NO':
        return 'New Orleans Saints'
    if initial is 'TB':
        return 'Tampa Bay Buccaneers'
    if initial is 'ARI':
        return 'Arizona Cardinals'
    if initial is 'SEA':
        return 'Seattle Seahawks'
    if initial is 'SF':
        return 'San Francisco 49ers'
    if initial is 'STL':
        return 'St. Louis Rams'

"""
Loads data into db. We only consider seasons from 2002-2013 as to avoid conflicts with teams
switching cities/names.
"""
def main():
    # TODO invoke your code to load the data into the database
    print("Creating Schema")
    cursor.execute(open("schema.sql", "r").read())
    print("Loading data")
    # Load data into Conferences table
    print("Inserting data into conference table")
    insert_query = "INSERT INTO Conference (name) VALUES (%(name)s) ON CONFLICT DO NOTHING"
    cursor.execute(insert_query, dict(name="AFC"))
    cursor.execute(insert_query, dict(name="NFC"))
    conn.commit()
    print("Done inserting data into conference table")
    # Load data into Division table
    print("Inserting data into division table")
    insert_query = "INSERT INTO Division (name) VALUES (%(name)s) ON CONFLICT DO NOTHING"
    cursor.execute(insert_query, dict(name="AFC East"))
    cursor.execute(insert_query, dict(name="AFC North"))
    cursor.execute(insert_query, dict(name="AFC South"))
    cursor.execute(insert_query, dict(name="AFC West"))
    cursor.execute(insert_query, dict(name="NFC East"))
    cursor.execute(insert_query, dict(name="NFC North"))
    cursor.execute(insert_query, dict(name="NFC South"))
    cursor.execute(insert_query, dict(name="NFC West"))
    conn.commit()
    print("Done inserting data into division table")
    # Load data into Team table
    print("Inserting data into team table")
    insert_query = "INSERT INTO Team (initial, name, conference, division)" \
    " VALUES(%(init)s, %(name)s, %(conference)s, %(division)s) ON CONFLICT DO NOTHING"
    with open('datasets/standings.csv') as standings:
        reader = csv.reader(standings, delimiter=',')
        first = next(reader)
        for row in reader:
            if int(row[0]) <= 2013:
                get_div = "SELECT id FROM division WHERE name=%(name)s"
                cursor.execute(get_div, dict(name=row[2]))
                div = cursor.fetchall()[0][0]
                get_conf = "SELECT id FROM conference WHERE name=%(name)s"
                cursor.execute(get_conf, dict(name=row[1]))
                conf = cursor.fetchall()[0][0]
                cursor.execute(insert_query, dict(init=row[3], name=init_to_name(row[3]), conference=conf,
                division=div))
                conn.commit()
            else:
                break
    print("Finished addding teams")
    # Load data into Game table
    # print("Inserting data into Game table")
    # insert_query = "INSERT INTO Game (home_team, away_team, home_score, away_score, game_date)" \
    # " VALUES(%(home)s, %(away)s, %(hs)s, %(as)s, %(date)s)"
    # with open('datasets/weather_20131231.csv')
    # Load data into Weather table

    # Load data into Standings table

    # Load data into Statistics table

    # Load data into Playoffs table


if __name__ == "__main__":
    main()
