import psycopg2
import datetime
    
class DataBase():
    def __init__(self):
        self.conn = psycopg2.connect(
                    database="exchange",
                    host="localhost",
                    user="odoo",
                    password="odoo",
                    port="5432")
        self.cur = self.conn.cursor()
        self.cur.execute("CREATE TABLE IF NOT EXISTS exchange_table(id serial PRIMARY KEY, Currency varchar , Value real, Date timestamp);")
        self.conn.commit()

    def insert_one(self, currency, value, _date):
        if not currency or not value:
            return False
        if not _date:
            _date = datetime.date.today()
        self.cur.execute('''INSERT INTO exchange_table (Currency , Value , Date) VALUES (\'{currency}\',{value},\'{_date}\');
                '''.format(currency=currency, value=value, _date=_date))
        self.conn.commit()

    def update_one(self, currency, value, _date=False):
        if not currency or not value:
            return False
        if not _date: 
            date = datetime.date.today()
        try:
            self.cur.execute(''' SELECT * from exchange_table WHERE Currency=\'{Currency}\' AND Date=\'{Date}\';'''.format(Currency=currency,Date=_date))
            res = self.cur.fetchone()
        except psycopg2.errors.UndefinedColumn:
            self.conn.rollback()
            res = False
        if res:
            self.cur.execute(''' UPDATE exchange_table SET Value=\'{Value}\'
                                WHERE id={id};'''.format(id=res[0], Value=value))
            self.conn.commit()
        else:
            self.insert_one(currency, value, _date)
        print(self.get_all())

    def get_all(self,_date=False):
        if _date:
            self.cur.execute(''' SELECT * from exchange_table WHERE Date=\'{Date}\';'''.format(Date=_date))
        else:
            self.cur.execute(''' SELECT * from exchange_table;''')
        return self.cur.fetchall()

