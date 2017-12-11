import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

print sys.argv


input = sys.argv[1].split(",")

if len(input) != 2:
    print 'must insert two integers'

int_1, int_2 = int(input[0]), int(input[1])

conn = psycopg2.connect(database="tcount", user="postgres", password="password", host="localhost", port="5432")
cur = conn.cursor()

cur.execute("SELECT word, count FROM Tweetwordcount WHERE count>= %s and count <=%s;" %  (int_1, int_2) )

records = cur.fetchall()
for record in records:
    print(record[0], record[1] )

cur.close()
conn.close()

