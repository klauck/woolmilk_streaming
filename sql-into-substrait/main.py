import duckdb

# reference https://github.com/substrait-io/duckdb-substrait-extension?tab=readme-ov-file

con = duckdb.connect()
con.execute("INSTALL substrait FROM community;")
con.execute("LOAD substrait;")

con.execute(query='CREATE TABLE crossfit (exercise text,difficulty_level int);')
con.execute(query="INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);")

substrait_json = con.execute("CALL get_substrait_json('select count(exercise) as exercise from crossfit where difficulty_level <=5');")

# print full
json = substrait_json.fetchnumpy()

# print only the result
print(json)