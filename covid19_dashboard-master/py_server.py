from flask import Flask ,request, jsonify, make_response
from sqlalchemy import create_engine, update, select, insert
app = Flask(__name__)
import json
from cov19Engine import Cov19

@app.route('/')
def index():
    return 'index'

def _build_cors_prelight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add('Access-Control-Allow-Headers', "*")
    response.headers.add('Access-Control-Allow-Methods', "*")
    return response

def _corsify_actual_response(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


def top10ConfirmrdCountries():
    data=c19.highest_10_cntrs_confirmed.to_dict('r')
    return _corsify_actual_response(jsonify(data))

def top10DeceasedCountries():
    data=c19.highest_10_cntrs_deceased.to_dict('r')
    return _corsify_actual_response(jsonify(data))

def totalValues():
    data = {'total confirmed': int(c19.df_confirmed_total[-1]),
            'total deceased': int(c19.df_deaths_total[-1]),
            'total recovered': int(c19.df_recovered_total[-1])
            }
    return _corsify_actual_response(jsonify(data))

def confTot():
    data = {'total confirmed': int(c19.df_confirmed_total[-1])}
    return _corsify_actual_response(jsonify(data))

def deathTot():
    data = {'total deceased': int(c19.df_deaths_total[-1])}
    return _corsify_actual_response(jsonify(data))

def recovTot():
    data={'total recovered' : int(c19.df_recovered_total[-1])}
    return _corsify_actual_response(jsonify(data))


def lineChartDaily():
    conf = c19.df_confirmed_daily
    death = c19.df_deaths_daily
    reco = c19.df_recovered_daily

    if (list(conf.index.values) == list(death.index.values) == list(reco.index.values)):
        print("plot indexes are equal")
        data={'dates': (list(conf.index.values)),
              'conf': conf.tolist(),
              'death': death.tolist(),
              'recovered': reco.tolist()}
        return _corsify_actual_response(jsonify(data))
    else:
        print(len(list(conf.index.values)))
        print(len(list(death.index.values)))
        print(len(list(reco.index.values)))
        data = {'dates': [],
                'conf': [],
                'death': [],
                'recovered': []}
        return _corsify_actual_response(jsonify(data))

def lineChartCumulative():
    conf = c19.df_confirmed_total
    death = c19.df_deaths_total
    reco = c19.df_recovered_total

    if (list(conf.index.values) == list(death.index.values) == list(reco.index.values)):
        print("plot indexes are equal")
        data={'dates': (list(conf.index.values)),
              'conf': conf.tolist(),
              'death': death.tolist(),
              'recovered': reco.tolist()}
        return _corsify_actual_response(jsonify(data))
    else:
        print(len(list(conf.index.values)))
        print(len(list(death.index.values)))
        print(len(list(reco.index.values)))
        data = {'dates': [],
                'conf': [],
                'death': [],
                'recovered': []}
        return _corsify_actual_response(jsonify(data))


app.add_url_rule('' + '/top10ConfirmrdCountries', 'top10ConfirmrdCountries', top10ConfirmrdCountries, methods=['OPTIONS','POST','GET'])
app.add_url_rule('' + '/top10DeceasedCountries', 'top10DeceasedCountries', top10DeceasedCountries, methods=['OPTIONS','POST','GET'])
# app.add_url_rule('' + '/confTot', 'confTot', confTot, methods=['OPTIONS','POST','GET'])
# app.add_url_rule('' + '/deathTot', 'deathTot', deathTot, methods=['OPTIONS','POST','GET'])
# app.add_url_rule('' + '/recovTot', 'recovTot', recovTot, methods=['OPTIONS','POST','GET'])
app.add_url_rule('' + '/totalValues', 'totalValues', totalValues, methods=['OPTIONS','POST','GET'])
app.add_url_rule('' + '/lineChartDaily', 'lineChartDaily', lineChartDaily, methods=['OPTIONS','POST','GET'])
app.add_url_rule('' + '/lineChartCumulative', 'lineChartCumulative', lineChartCumulative, methods=['OPTIONS','POST','GET'])



if __name__=='__main__':
    c19 = Cov19()
    app.run(host='0.0.0.0',port=5050,debug=True,use_reloader=True,processes=1,static_files={'/':'dist'})
