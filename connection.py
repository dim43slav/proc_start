import psycopg2 as pc

class Connection:
    def __init__(self):
        self.conn = pc.connect(
        host="10.76.17.67",
        database="dbtest",
        user="ikurdin",
        password="Init1234",
        port = "5432")

    def connect(self):
        self.cur = self.conn.cursor()

    def query(self, query_string, query_type):
        self.connect()

        if query_type == 'S':
            self.cur.execute(query_string)
            result = self.cur.fetchall()
            self.disconnect()
            return result
        else: 
            self.cur.execute(query_string)
            self.conn.commit()
            self.disconnect()

    
    def disconnect(self):
        self.conn.close()
        return 0