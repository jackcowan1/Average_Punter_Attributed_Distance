import csv
import sqlite3
import requests
from bs4 import BeautifulSoup

# create the database
conn = sqlite3.connect('NFL_ST_data')

#Make games table
CG = """
Create table if not exists Games (
id int,
season int,
week int,
gameDate date,
gameTime time,
home text,
away text,
weather text,
primary key(id)
);
"""

IG = "Insert into Games (id, season, week, gameDate, gameTime, home, away, weather) Values (?,?,?,?,?,?,?,?)"

conn.execute(CG)

#add weather
abbrs = {'ATL':'falcons', 'BUF':'bills', 'PIT':'steelers', 'CIN':'bengals', 'TEN':'titans','HOU':'texans','TB':'buccaneers','JAX':'jaguars','WAS':'redskins',
         'DAL':'cowboys','CHI':'bears', 'NYJ':'jets','LA':'rams','BAL':'ravens','CAR':'panthers','LAC':'chargers','MIN':'vikings', 'CLE':'browns','MIA':'dolphins',
         'KC':'chiefs', 'PHI':'eagles', 'IND':'colts', 'ARI':'cardinals', 'DET':'lions', 'OAK':'raiders', 'NE':'patriots', 'NYG':'giants','SEA':'seahawks',
         'NO':'saints','DEN':'broncos', 'SF':'49ers', 'GB':'packers', 'LV':'raiders'}

try:
    with open("Data/games.csv") as csv_file:
        data = csv.reader(csv_file)
        next(data)
        weather = ''
        for row in data:
            if row[5] in {'LV', 'LA', 'MIN', 'DET', 'NO', 'ATL', 'DAL', 'IND', 'ARI', 'HOU'}:
                weather = 'dome'
            elif (row[1] == 2020) and (row[5] in {'LA', 'LAC'}):
                weather = 'dome'
            else:
                if (row[0] == '2020092702') or (row[0] == '2020122010'): #mark where weather website switches to calling the team "football team"
                    abbrs['WAS'] = 'football%20team'
                if (row[0] == '2020120702') or (row[0] == '2020122710'): # mark where it switches again to "washington
                    abbrs['WAS'] = 'washington'
                url = f"https://www.nflweather.com/games/{row[1]}/week-{row[2]}/{abbrs[row[6]]}-at-{abbrs[row[5]]}"
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                try:
                    weather = soup.find_all(class_='fw-bold text-wrap')[0].text[:-1]
                except:
                    print(row[0])
                    print(url)

            conn.execute(IG, [row[0], row[1], row[2], row[3], row[4], row[5], row[6], weather])
    conn.commit()
except sqlite3.IntegrityError as e:
    pass

#Make players table


CPlayers = """
Create table if not exists Players(
id int,
height int,
weight int,
birthday date,
college text,
position text,
name text,
primary key (id));
"""
IPlayers = "Insert into Players values (?,?,?,?,?,?,?)"

try:
    conn.execute(CPlayers)
except sqlite3.IntegrityError as e:
    pass

with open('Data/players.csv') as csv_file:
    data = csv.reader(csv_file)
    next(data)
    for row in data:
        heights = row[1].split('-')
        height = (int(heights[0])*12) + (int(heights[1])) if len(heights) > 1 else heights[0]
        try:
            conn.execute(IPlayers, [row[0], height, row[2], row[3], row[4], row[5], row[6]])
        except sqlite3.IntegrityError as e:
            pass

conn.commit()

#make tables from plays data

Cplays = """
Create table if not exists Plays(
gameId int,
playId int,
playDescription text,
quarter int,
down int,
yardsToGo int,
possessionTeam text,
specialTeamsPlayType text,
specialTeamsResult text,
kickerId int,
kickBlockerId int,
yardlineSide text,
yardlineNumber int,
gameClock time,
penaltyYards int,
preSnapHomeScore int,
preSnapVisitorScore int,
passResult text,
kickLength int,
kickReturnYardage int,
playResult int,
absoluteYardlineNumber int,
primary key(gameId, playId),
foreign key (gameId) references Games(id),
foreign key (kickerId) references Players(id),
foreign key (kickBlockerId) references Players(id)
);
"""

CR = """
Create Table if not exists Returners(
gameId int,
playId int,
returner int,
primary key(gameId, playId, returner),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId),
foreign key (returner) references Players(id)
);
"""

CPC = """
Create table if not exists Penalty_codes(
gameId int,
playId int,
penalty text,
primary key(gameId, playId, penalty),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""

CPP = """
Create Table if not exists Penalty_players(
gameId int,
playId int,
player int,
primary key(gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId),
foreign key (player) references Players(id)
);
"""

for i in [Cplays, CR, CPC, CPP]:
    try:
        conn.execute(i)
    except sqlite3.IntegrityError as e:
        pass
conn.commit()


IPlays = "Insert into Plays values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
IR = "Insert into Returners values (?,?,?)"
IPC = "Insert into Penalty_codes values (?,?,?)"
IPP = "Insert into Penalty_players values (?,?,?)"

play_sub = {IR:10, IPC:15, IPP:16}


with open("Data/plays.csv") as csv_file:
    data = csv.reader(csv_file)
    next(data)
    for row in data:
        gameId = row[0]
        playId = row[1]
        try:
            conn.execute(IPlays, [row[i] for i in [0,1,2,3,4,5,6,7,8,9,11,12,13,14,17,18,19,20,21,22,23,24]])
        except sqlite3.IntegrityError as e:
            pass
        for insert in play_sub.keys():
            vals = row[play_sub[insert]].split(';')
            if vals[0] != 'NA':
                for value in vals:
                    try:
                        conn.execute(insert, [gameId, playId, value])
                    except sqlite3.IntegrityError as e:
                        pass

conn.commit()



#turn stats data into tables

CS = """
Create table if not exists Stats (
gameId int,
playId int,
snapDetail text,
snapTime float, 
operationTime float,
hangTime float, 
kickType char,
kickDirectionIntended char,
kickDirectionActual char,
returnDirectionIntended char,
returnDirectionActual char,
tackler text,
kickoffReturnFormation text,
kickContactType text,
primary key (gameId, playID),
foreign key (gameId) references Games(id)
foreign key (playId) references Plays(playId)
);
"""

IS = "Insert into Stats values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

CMT = """
Create table if not exists Missed_tackles(
gameId int,
playId int,
player text,
primary key (gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""
CAT = """
Create table if not exists Assist_tackles(
gameId int,
playId int,
player text,
primary key (gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""
CG = """
Create table if not exists Gunners(
gameId int,
playId int,
player text,
primary key (gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""
CPR = """
Create table if not exists Punt_Rushers(
gameId int,
playId int,
player text,
primary key (gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""
CSAF = """
Create table if not exists Safties(
gameId int,
playId int,
player text,
primary key (gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""
CV = """
Create table if not exists Vises(
gameId int,
playId int,
player text,
primary key (gameId, playId, player),
foreign key (gameId) references Games(id),
foreign key (playId) references Plays(playId)
)
"""
IMT = "Insert into Missed_tackles (gameId, playId, player) VALUES (?,?,?)"
IAT = "Insert into Assist_tackles (gameId, playId, player) VALUES (?,?,?)"
IG = "Insert into Gunners (gameId, playId, player) VALUES (?,?,?)"
IPR = "Insert into Punt_Rushers (gameId, playId, player) VALUES (?,?,?)"
ISAF = "Insert into Safties (gameId, playId, player) VALUES (?,?,?)"
IV = "Insert into Vises (gameId, playId, player) VALUES (?,?,?)"

for i in [CS, CMT, CAT, CG, CPR, CSAF, CV]:
    conn.execute(i)

multi = {IMT: 11, IAT: 12, IG:15, IPR:16, ISAF: 17, IV:18}


with open('Data/PFFScoutingData.csv') as csv_file:
    data = csv.reader(csv_file)
    next(data)
    for row in data:
        gameId = row[0]
        playId = row[1]
        try:
            conn.execute(IS, [row[x] for x in [0,1,2,3,4,5,6,7,8,9,10,13,14,19]])
        except sqlite3.IntegrityError as e:
            pass
        for insert in multi.keys():
            vals = row[multi[insert]].split('; ')
            if vals[0] != 'NA':
                for player in vals:
                    try:
                        conn.execute(insert, [gameId, playId, player])
                    except sqlite3.IntegrityError as e:
                        pass
conn.commit()

# Make table for tracking data
CT = """
Create Table if not exists Tracking(
time time,
x float,
y float,
s float,
a float,
dis float,
o float,
dir float,
event text,
nflId int,
name text,
jersey int,
position text,
team text,
frameId int,
gameId int,
playId int,
playDirection text,
primary key(gameId, playId, frameId, nflId),
foreign key(gameId) references Games(id),
foreign key(playId) references Plays(playId),
foreign key(nflId) references Players(id)
)
"""
IT = "Insert into Tracking values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

try:
    conn.execute(CT)
    for tracking in ['Data/tracking2018.csv', 'Data/tracking2019.csv', 'Data/tracking2020.csv']:
        with open(tracking) as csv_file:
            data = csv.reader(csv_file)
            next(data)
            conn.executemany(IT, data)

        print("Tracking file done")

    conn.commit()

except sqlite3.IntegrityError as e:
    pass





