from flask import Flask, request, flash, url_for, redirect, render_template
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from Parser import ExchangeAPI

app = Flask (__name__)
app.config ['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://odoo:odoo@localhost/exchange'
db = SQLAlchemy(app)
Parser = ExchangeAPI()

class currency(db.Model):
    __tablename__ = 'currency'
    id = db.Column('id', db.Integer, primary_key = True)
    code = db.Column(db.String(6), nullable=False)
    name = db.Column(db.String(100))
    currency_value_ids = db.relationship('values', backref='currency')

    def __repr__(self):
        return f"<{self.code}>"

class values(db.Model):
    __tablename__ = 'values'
    id = db.Column(db.Integer, primary_key = True)
    value = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    currency_id = db.Column(db.Integer, db.ForeignKey('currency.id'))


@app.route('/', methods = ['GET', 'POST'])
def show_all():
    if request.form.get('search'):
        return show_one_intime(request.form.get('search'))
        # if the search function needs to search the db, uncomment this block of code
        # return  render_template("show_all.html", 
        #         currency_list=currency.query.like(code = request.form.get('search')))
    elif request.form.get('get_values'):
        parce_now()
    return render_template('show_all.html', currency_list = currency.query.all() )


@app.route('/currency-<code>', methods = ['GET', 'POST'])
def show_one(code):
    exch = currency.query.filter_by(code=code).first()
    vals  = values.query.filter_by(currency_id=exch.id).all()
    print(exch.currency_value_ids)
    return render_template('currency.html', currency_list=vals, code=code)

@app.route('/intime/<code>', methods = ['GET', 'POST'])
def show_one_intime(code):
    exch = Parser.get_one_exchange_value(code)
    if exch.get('rates'):
        base=exch.get('base')
        for key,val in exch.get('rates').items():
            cur, value = key, val
    else:
        base='USD'
        cur=code
        value="{} is not a code of currency".format(cur)
    return render_template('show_one.html', base=base, cur=cur, value=value )

def parce_now():
    ex_dict = Parser.get_exchange_values()
    if ex_dict:
        _date = ex_dict.get('date')
        for key, value in ex_dict.get('rates').items():
            print(key, value)
            exch = currency.query.filter_by(code=key).first()
            if not exch:
                c = currency(code=key)
                db.session.add(c)
                db.session.commit()
                exch = currency.query.filter_by(code=key).first()
            val = values.query.filter_by(date=_date, currency_id=exch.id).first()
            if not val:
                vall = values(currency_id=exch.id,value=value,date=_date)
                db.session.add(vall)
                db.session.commit()
                val = values.query.filter_by(date=_date, currency_id=exch.id).first()
            elif val.value != value:
                val.value=value
                db.session.commit()


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug = True)