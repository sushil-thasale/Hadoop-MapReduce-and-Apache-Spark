master = load 'baseball/Master.csv' using PigStorage(',');
--describe master
onlyMasterData = filter master by $0 != 'playerID';
--describe onlyMasterData
requiredMasterData = foreach onlyMasterData generate $0 as playerID, $13 as fname, $14 as lname;
--describe requiredMasterData


-- extracting and refining salaries data
salaries = load 'baseball/Salaries.csv' using PigStorage(',');
--describe salaries
onlySalariesData = filter salaries by $0 != 'yearID';
--describe onlySalariesData
requiredSalariesData = foreach onlySalariesData generate $0 as yearID, $1 as teamID, $3 as playerID, $4 as salary;
--describe requiredSalariesData


-- extracting and refining batting data
batting = load 'baseball/Batting.csv' using PigStorage(',');
--describe batting
onlyBattingData = filter batting by $0 != 'playerID';
--describe onlyBattingData
battingDataWithHR = filter onlyBattingData by (SIZE($11)>=0);
requiredBattingData = foreach battingDataWithHR generate $0 as playerID, $1 as yearID, $3 as teamID, $11 as hr ;
--describe requiredBattingData


-- extracting and refining teams data
teams = load 'baseball/Teams.csv' using PigStorage(',');
--describe teams
onlyTeamsData = filter teams by $0 != 'yearID';
--describe onlyTeamsData
requiredTeamsData = foreach onlyTeamsData generate $0 as yearID, $2 as teamID, $40 as tname;
--describe requiredSalariesData


-- join salary and batting 
batting_salary = join requiredSalariesData by (playerID,yearID,teamID), requiredBattingData by (playerID,yearID,teamID);
refined_batting_salary = foreach batting_salary generate $0, $1, $2, $3, $7;

-- join (batting-salary) with master
bat_sal_master = join refined_batting_salary by playerID, requiredMasterData by playerID;
refined_bat_sal_master = foreach bat_sal_master generate $0, $1, $2, $3, $4, $6, $7;

-- select required columns
playerData = foreach refined_bat_sal_master generate $0, $1, $2, $3, $4,($3 / $4), $5, $6;

-- join (batting, salary, master) with teams
team_player_data = join playerData by (yearID, teamID), requiredTeamsData by (yearID, teamID);
refined_team_player_data = foreach team_player_data generate $6, $7, $10, $3, $4, $5;

unorderedop = FILTER refined_team_player_data BY ($5!=0);
orderedop = ORDER unorderedop BY $5 ASC; 
top5 = limit orderedop 5; 
STORE top5 INTO 'result' using PigStorage(',');
