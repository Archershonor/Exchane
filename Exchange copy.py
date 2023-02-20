import requests
import sqlite3
from bs4 import BeautifulSoup
from tqdm import tqdm
from flask import Flask, render_template, url_for, request
# import pandas as pd


"""     База Данных      """
class DB:

    def __init__(self, deps=dict()):
        self.conn = sqlite3.connect('Exchange.db', check_same_thread = False)
        self.c = self.conn.cursor()
        self.cur_deps = False
        self.limit=200

        str_deps= ''.join(['\n, \'%s\' text' % (dep) for dep in deps])
        self.c.execute(''' CREATE TABLE IF NOT EXISTS Exchange(
                                    id integer primary key,
                                    name text,
                                    date text,
                                    result text
                                    {}
                                    );'''.format(str_deps)
                    )
        self.conn.commit()
        self.get_table()

    def insert_all(self, _id, name,  date, result, deps_dict):
        str_deps, str_goloses = '',''
        self.c.execute(''' SELECT * from Exchange WHERE id={}'''.format(_id))
        res = self.c.fetchone()
        if res:
            str_deps = ''.join(['\"%s\" = \"%s\" ,' % (dep, golos) for dep, golos in deps_dict.items()])[:-1:]
            self.c.execute(''' UPDATE Exchange  SET 
                            name=\"{name}\", date=\"{date}\", 
                            result=\"{result}\", {str_deps}
                            ;'''.format(id=_id,name=name, 
                            date=date, result=result, str_deps=str_deps)
                        )
            self.conn.commit()

        else:
            try:
                for dep, golos in deps_dict.items():
                    str_deps += '\"{}\", '.format(dep)
                    str_goloses += '\"{}\", '.format(golos)

                self.c.execute(''' INSERT INTO Exchange (id, name, date, result, {str_deps}) 
                            VALUES ({id}, \"{name}\", \"{date}\", \"{result}\",{str_goloses});
                            '''.format(id=_id,name=name.replace('\"','\''), date=date, result=result,
                                        str_deps=str_deps[:-2:], str_goloses=str_goloses[:-2:])
                            )
                self.conn.commit()

            except sqlite3.OperationalError:
                self.get_deps()
                for dep in deps_dict.keys():
                    if not dep in self.cur_deps:
                        self.insert_dep(dep)
                with open('added_deps.txt', 'a') as f:
                    f.write('{}, \n'.format(dep))
                self.insert_all(_id, name,  date, result, deps_dict)


    def insert_name(self, _id, name, date, result):
        self.c.execute(''' SELECT * from Exchange WHERE id={}'''.format(_id))
        res = self.c.fetchone()

        if res:
            return
        self.c.execute(''' INSERT INTO Exchange (id, name, date, result) 
                        VALUES ({id}, \"{name}\", \"{date}\", \"{result}\");
                        '''.format(id=_id,name=name, date=date, result=result)
                        )
        self.conn.commit()


    def insert_golos(self, name,dep,golos):
        try:
            self.c.execute(''' UPDATE Exchange 
                            SET \'{dep}\'=\'{golos}\' 
                            WHERE name=\"{name}\"
                            '''.format(name=name,
                            dep=dep, golos=golos)
                        )
            self.conn.commit()
        except:
            self.get_deps()
            self.insert_dep(dep)
            self.insert_golos(name,dep,golos)


    def get_deps(self):
        
        self.c.execute('''pragma table_info(Exchange);''')
        self.cur_deps = [ c[1] for c in self.c.fetchall()]

        return(self.cur_deps)

    def search_deps(self, search=''):
        self.c.execute('''pragma table_info(Exchange);''')

        return filter(lambda x: search in x, [ c[1] for c in self.c.fetchall()])

    def get_table(self):
        if not self.cur_deps:
            self.get_deps()
        self.c.execute('''select * from Exchange order by id DESC limit {};'''.format(self.limit))
        self.table = self.c.fetchall()
        self.table = list(map(list, zip(*self.table)))
        self.table = {self.cur_deps[x]:self.table[x] for x in range(len(self.cur_deps))}

    def insert_dep(self, dep):
        if not (dep in self.cur_deps):
            self.c.execute('ALTER TABLE Exchange ADD COLUMN \"{dep}\";'.format(dep=dep))
            self.conn.commit()


    def get_last_id(self):
        self.c.execute('SELECT id FROM Exchange ORDER BY id DESC;')
        return self.c.fetchone()[0]


    def dep_to_dep(self, master, slave, limit):
        if limit:
            limit = 'LIMIT {}'.format(limit)
        self.c.execute('''  SELECT id, name, Exchange.\'{master}\',
                            Exchange.\'{slave}\'
                            FROM Exchange 
                            WHERE SUBSTR(Exchange.\'{master}\', 1, 2) 
                            NOT LIKE SUBSTR(Exchange.\'{slave}\', 1, 2)
                            ORDER BY ID DESC {limit}
                        ;'''.format(master=master, slave=slave, limit=limit))
        another = self.c.fetchall()
        self.c.execute('''SELECT count(*) FROM Exchange {limit} ;'''.format(limit=limit))
        pull = self.c.fetchone()[0]
        return {'pull': pull, 'another': another, 'limit': limit}
         

    # def dep_to_all(self, master):
    #     list_ = list()
    #     pull = len(self.DF)
    #     lab = lambda x: x[0]==x[1]
    #     for slave in tqdm(self.DF.columns):
    #         res = len(list(filter(lab,zip(self.DF[slave], self.DF[master]))))
    #         list_.append([slave, int(res / pull * 100)])

    #     def keyFunc(item):
    #         return item[1]
    #     list_.sort(key=keyFunc)

    #     return list_[::-1]
         

    def dep_to_all(self, master):
        if not self.table:
            self.get_table()
        list_ = list()
        pull = len(self.table[master])
        lab = lambda x: x[0]==x[1]
        for key, value in self.table.items():
            res = len(list(filter(lab,zip(self.table[master][:-4:], value[:-4:]))))
            list_.append([key, int(res / pull * 100)])
        def keyFunc(item):
            return item[1]
        list_.sort(key=keyFunc)

        return list_[::-1]

"""     Парсер HTML      """
class Parser:
    def __init__(self, sql=False):

        self.id = 24
        self.HEADER = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 YaBrowser/20.12.3.140 Yowser/2.5 Safari/537.36'
        }

        if sql:

            parsed = False
            while not parsed:
                self.id +=1
                parsed = self.parse_by_fr()

            self.sql = DB(parsed["dict_"])
            
            self.process_single_html()
        
        else:
            self.sql = DB()


    def parse_by_fr(self):

        self.URL = 'http://w1.c1.rada.gov.ua/pls/radan_gs09/ns_golos?g_id={}'.format(self.id)
        responce = requests.get(self.URL, headers=self.HEADER)
        if responce.ok:

            soup = BeautifulSoup(responce.content, 'html.parser')
            dict_ = dict()

            header = soup.find('div', class_='head_gol')
            name = header.text.split('\n')[1]
            try:
                date = header.text.split('\n')[3]
                res = header.text.split('\n')[-2]
            except:
                date, res = '',''


            all_fr = soup.find('ul', class_='fr')
            for i in all_fr.li.find_all('li'):
                dep = i.find('div', class_='dep').text
                golos = i.find('div', class_='golos').text
                dict_[dep] = golos
            return {"name":name,"date":date, "res": res, "dict_":dict_}


    def process_single_html(self):
        parsed = self.parse_by_fr()
        if parsed:
            self.sql.insert_all(_id=self.id, name=parsed['name'], date=parsed['date'], 
                                result=parsed['res'], deps_dict=parsed["dict_"])

            # self.sql.insert_name(_id=self.id, name=parsed['name'], 
            #                     date=parsed['date'], result=parsed['res'])
            # for dep, golos in parsed["dict_"].items():
            #     self.sql.insert_golos(name=parsed["name"], dep=dep, golos=golos)
            

    def process_multi_html(self, iters=50):
        lastrowid = self.sql.get_last_id()
        for _id in tqdm(range(lastrowid, lastrowid + iters)):
            self.id = _id
            self.process_single_html()


"""     Сайт  Flask      """
Parser = Parser()
api = Flask(__name__)
api.config['DEBUG'] = True
api.config['TESTING'] = True

@api.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # header_post(request.form)
        if request.form.get('limit'):
            Parser.sql.limit = request.form['limit']
        elif request.form.get('h-limit'):
            Parser.sql.limit = request.form['h-limit']
        elif request.form.get('search'):
            return render_template("index.html", 
                        deps=Parser.sql.search_deps(request.form.get('search')))
    Parser.sql.get_deps()
    return render_template("index.html", deps=Parser.sql.cur_deps[4::])

@api.route("/dep_to_dep/<string:master>/<string:slave>", methods=['GET', 'POST'])
def dep_to_dep(master, slave, limit=''):
    if request.method == 'POST':
        if request.form.get('limit'):
            limit = request.form.get('limit')
        elif request.form.get('h-limit'):
            Parser.sql.limit = request.form['h-limit']
        elif request.form.get('search'):
            return render_template("index.html", 
                        deps=Parser.sql.search_deps(request.form.get('search')))
    
    dict_ = Parser.sql.dep_to_dep(master=master, slave=slave, limit = limit)
    pull = dict_['pull']
    another = dict_['another']
    return render_template("dep_to_dep.html", master=master, slave=slave, 
            pull=pull, another=another, len_another=len(another), limit = limit,
            percent=int((pull - len(another)) /pull * 100))


@api.route("/dep_to_all/<string:master>", methods=['GET', 'POST'])
def dep_to_all(master):
    if request.method == 'POST':
        if request.form.get('limit'):
            Parser.sql.limit = request.form['limit']
        elif request.form.get('h-limit'):
            Parser.sql.limit = request.form['h-limit']
        elif request.form.get('search'):
            return render_template("index.html", 
                        deps=Parser.sql.search_deps(request.form.get('search')))
        Parser.sql.get_table()
    return render_template("dep_to_all.html",
                             master=master, limit=Parser.sql.limit,
                            deps=Parser.sql.dep_to_all(master=master))

@api.route("/dep/<string:master>", methods=['GET', 'POST'])
def dep(master):
    if request.method == 'POST':
        if request.form.get('limit'):
            Parser.sql.limit = request.form['limit']
        elif request.form.get('h-limit'):
            Parser.sql.limit = request.form['h-limit']
        elif request.form.get('search'):
            return render_template("dep.html", master=master,
                        deps=Parser.sql.search_deps(request.form.get('search')))

    return render_template("dep.html", master=master,
                            deps=Parser.sql.cur_deps[4::])

if __name__ == '__main__':
    api.run()
    # Parser = Parser(sql = True)
    # Parser.process_multi_html(iters=200)


