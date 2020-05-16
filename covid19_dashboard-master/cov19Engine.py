import pandas as pd
import numpy as np
import datetime

class Cov19:
    def __init__(self):
        self.df_confirmed = None
        self.df_deaths = None
        self.df_recovered = None
        self.get_updated_datasets()

        self.df_confirmed_daily = None
        self.df_deaths_daily = None
        self.df_recovered_daily = None
        self.df_confirmed_total = None
        self.df_deaths_total = None
        self.df_recovered_total = None
        self.total_case_agg()




        self.df_deaths_confirmed_sorted_total = None
        self.df_recovered_sorted_total = None
        self.df_confirmed_sorted_total = None
        self.highest_10_cntrs_confirmed = None
        self.highest_10_cntrs_deceased = None
        self.map_data = None
        self.preprocessing_req_info()

    def get_updated_datasets(self):
        # url_all = 'https://data.world/covid-19-data-resource-hub/covid-19-case-counts/workspace/file?filename=COVID-19+Cases.csv'
        url_confirmed = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
        url_deaths = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
        url_recovered = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

        # df_all = pd.read_csv(url_all)
        df_confirmed = pd.read_csv(url_confirmed)
        df_deaths = pd.read_csv(url_deaths)
        df_recovered = pd.read_csv(url_recovered)

        self.df_confirmed = df_confirmed
        self.df_deaths = df_deaths
        self.df_recovered = df_recovered

    def df_move1st_sg(self, df_t):
        # Moving India to the first row in the datatable
        df_t["new"] = range(1, len(df_t) + 1)
        df_t.loc[df_t[df_t['Country/Region'] == 'India'].index.values, 'new'] = 0
        df_t = df_t.sort_values("new").drop('new', axis=1)
        return df_t

    def total_case_agg(self):
        # Total
        df_confirmed_total = self.df_confirmed.iloc[:, 4:].sum(axis=0)
        df_deaths_total = self.df_deaths.iloc[:, 4:].sum(axis=0)
        df_recovered_total = self.df_recovered.iloc[:, 4:].sum(axis=0)
        self.df_confirmed_total = df_confirmed_total
        self.df_deaths_total = df_deaths_total
        self.df_recovered_total = df_recovered_total

        self.df_confirmed_daily = df_confirmed_total.sub(df_confirmed_total.shift())
        self.df_confirmed_daily.iloc[0] = df_confirmed_total.iloc[0]

        self.df_deaths_daily = df_deaths_total.sub(df_deaths_total.shift())
        self.df_deaths_daily.iloc[0] = df_deaths_total.iloc[0]

        self.df_recovered_daily = df_recovered_total.sub(df_recovered_total.shift())
        self.df_recovered_daily.iloc[0] = df_recovered_total.iloc[0]

    def preprocessing_req_info(self):
        df_deaths_confirmed = self.df_deaths.copy()
        df_deaths_confirmed['confirmed'] = self.df_confirmed.iloc[:, -1]

        df_deaths_confirmed_sorted = df_deaths_confirmed.sort_values(by=df_deaths_confirmed.columns[-2], ascending=False)[['Country/Region', df_deaths_confirmed.columns[-2], df_deaths_confirmed.columns[-1]]]
        df_recovered_sorted = self.df_recovered.sort_values(by=self.df_recovered.columns[-1], ascending=False)[['Country/Region', self.df_recovered.columns[-1]]]
        df_confirmed_sorted = self.df_confirmed.sort_values(by=self.df_confirmed.columns[-1], ascending=False)[['Country/Region', self.df_confirmed.columns[-1]]]

        # Single day increase
        df_deaths_confirmed_sorted['24hr'] = df_deaths_confirmed_sorted.iloc[:, -2] - self.df_deaths.sort_values(by=self.df_deaths.columns[-1], ascending=False)[self.df_deaths.columns[-2]]
        df_recovered_sorted['24hr'] = df_recovered_sorted.iloc[:, -1] - self.df_recovered.sort_values(by=self.df_recovered.columns[-1], ascending=False)[self.df_recovered.columns[-2]]
        df_confirmed_sorted['24hr'] = df_confirmed_sorted.iloc[:, -1] - self.df_confirmed.sort_values(by=self.df_confirmed.columns[-1], ascending=False)[self.df_confirmed.columns[-2]]

        # Aggregate the countries with different province/state together
        df_deaths_confirmed_sorted_total = df_deaths_confirmed_sorted.groupby('Country/Region').sum()
        df_deaths_confirmed_sorted_total = df_deaths_confirmed_sorted_total.sort_values(by=df_deaths_confirmed_sorted_total.columns[0],
                                                                                        ascending=False).reset_index()
        df_recovered_sorted_total = df_recovered_sorted.groupby('Country/Region').sum()
        df_recovered_sorted_total = df_recovered_sorted_total.sort_values(by=df_recovered_sorted_total.columns[0],
                                                                          ascending=False).reset_index()
        df_confirmed_sorted_total = df_confirmed_sorted.groupby('Country/Region').sum()
        df_confirmed_sorted_total = df_confirmed_sorted_total.sort_values(by=df_confirmed_sorted_total.columns[0],
                                                                          ascending=False).reset_index()

        self.df_deaths_confirmed_sorted_total = df_deaths_confirmed_sorted_total
        self.df_recovered_sorted_total = df_recovered_sorted_total
        self.df_confirmed_sorted_total = df_confirmed_sorted_total


        self.df_recovered['Province+Country'] = self.df_recovered[['Province/State', 'Country/Region']].fillna('nann').agg('|'.join, axis=1)
        self.df_confirmed['Province+Country'] = self.df_confirmed[['Province/State', 'Country/Region']].fillna('nann').agg('|'.join, axis=1)
        df_recovered_fill = self.df_recovered
        df_recovered_fill.set_index("Province+Country")
        df_recovered_fill.set_index("Province+Country").reindex(self.df_confirmed['Province+Country'])
        df_recovered_fill = df_recovered_fill.set_index("Province+Country").reindex(self.df_confirmed['Province+Country']).reset_index()

        # split Province+Country back into its respective columns
        new = df_recovered_fill["Province+Country"].str.split("|", n=1, expand=True)
        df_recovered_fill['Province/State'] = new[0]
        df_recovered_fill['Country/Region'] = new[1]
        df_recovered_fill['Province/State'].replace('nann', 'NaN')

        # drop 'Province+Country' for all dataset
        self.df_confirmed.drop('Province+Country', axis=1, inplace=True)
        self.df_recovered.drop('Province+Country', axis=1, inplace=True)
        df_recovered_fill.drop('Province+Country', axis=1, inplace=True)

        # Data preprocessing for times series countries graph display
        # create temp to store sorting arrangement for all confirm, deaths and recovered.
        df_confirmed_sort_temp = self.df_confirmed.sort_values(by=self.df_confirmed.columns[-1], ascending=False)

        df_confirmed_t = self.df_move1st_sg(df_confirmed_sort_temp)
        df_confirmed_t['Province+Country'] = df_confirmed_t[['Province/State', 'Country/Region']].fillna('nann').agg('|'.join, axis=1)
        df_confirmed_t = df_confirmed_t.drop(['Province/State', 'Country/Region', 'Lat', 'Long'], axis=1).T

        df_deaths_t = self.df_deaths.reindex(df_confirmed_sort_temp.index)
        df_deaths_t = self.df_move1st_sg(df_deaths_t)
        df_deaths_t['Province+Country'] = df_deaths_t[['Province/State', 'Country/Region']].fillna('nann').agg('|'.join,axis=1)
        df_deaths_t = df_deaths_t.drop(['Province/State', 'Country/Region', 'Lat', 'Long'], axis=1).T

        # take note use reovered_fill df
        df_recovered_t = df_recovered_fill.reindex(df_confirmed_sort_temp.index)
        df_recovered_t = self.df_move1st_sg(df_recovered_t)
        df_recovered_t['Province+Country'] = df_recovered_t[['Province/State', 'Country/Region']].fillna('nann').agg('|'.join, axis=1)
        df_recovered_t = df_recovered_t.drop(['Province/State', 'Country/Region', 'Lat', 'Long'], axis=1).T

        df_confirmed_t.columns = df_confirmed_t.iloc[-1]
        df_confirmed_t = df_confirmed_t.drop('Province+Country')

        df_deaths_t.columns = df_deaths_t.iloc[-1]
        df_deaths_t = df_deaths_t.drop('Province+Country')

        df_recovered_t.columns = df_recovered_t.iloc[-1]
        df_recovered_t = df_recovered_t.drop('Province+Country')

        df_confirmed_t.index = pd.to_datetime(df_confirmed_t.index)
        df_deaths_t.index = pd.to_datetime(df_confirmed_t.index)
        df_recovered_t.index = pd.to_datetime(df_confirmed_t.index)

        # Highest 10 plot data preprocessing
        # getting highest 10 countries with confirmed case
        name = df_confirmed_t.columns.str.split("|", 1)
        df_confirmed_t_namechange = df_confirmed_t.copy()

        # name0 = [x[0] for x in name]
        name1 = [x[1] for x in name]
        df_confirmed_t_namechange.columns = name1
        df_confirmed_t_namechange = df_confirmed_t_namechange.groupby(df_confirmed_t_namechange.columns, axis=1).sum()
        df_confirmed_t_namechange10 = df_confirmed_t_namechange.sort_values(by=df_confirmed_t_namechange.index[-1],
                                                                            axis=1, ascending=False).iloc[:, :10]
        df_confirmed_t_stack = df_confirmed_t_namechange10.stack()
        df_confirmed_t_stack = df_confirmed_t_stack.reset_index(level=[0, 1])
        df_confirmed_t_stack.rename(columns={"level_0": "Date", 'level_1': 'Countries', 0: "Confirmed"}, inplace=True)
        self.highest_10_cntrs_confirmed = df_confirmed_t_stack

        # getting highest 10 countries with deceased case
        name = df_deaths_t.columns.str.split("|", 1)
        df_deaths_t_namechange = df_deaths_t.copy()

        name1 = [x[1] for x in name]
        df_deaths_t_namechange.columns = name1
        df_deaths_t_namechange = df_deaths_t_namechange.groupby(df_deaths_t_namechange.columns, axis=1).sum()
        df_deaths_t_namechange10 = df_deaths_t_namechange.sort_values(by=df_deaths_t_namechange.index[-1], axis=1,
                                                                      ascending=False).iloc[:, :10]
        df_deaths_t_stack = df_deaths_t_namechange10.stack()
        df_deaths_t_stack = df_deaths_t_stack.reset_index(level=[0, 1])
        df_deaths_t_stack.rename(columns={"level_0": "Date", 'level_1': 'Countries', 0: "Deceased"}, inplace=True)
        self.highest_10_cntrs_deceased = df_deaths_t_stack

        # Recreate required columns for map data
        map_data = self.df_confirmed[["Province/State", "Country/Region", "Lat", "Long"]]
        map_data['Confirmed'] = self.df_confirmed.loc[:, self.df_confirmed.columns[-1]]
        map_data['Deaths'] = self.df_deaths.loc[:, self.df_deaths.columns[-1]]
        map_data['Recovered'] = df_recovered_fill.loc[:, df_recovered_fill.columns[-1]]
        map_data['Recovered'] = map_data['Recovered'].fillna(0).astype(int)  # to covert value back to int and fillna with zero

        # last 24 hours increase
        map_data['Deaths_24hr'] = self.df_deaths.iloc[:, -1] - self.df_deaths.iloc[:, -2]
        map_data['Recovered_24hr'] = df_recovered_fill.iloc[:, -1] - df_recovered_fill.iloc[:, -2]
        map_data['Confirmed_24hr'] = self.df_confirmed.iloc[:, -1] - self.df_confirmed.iloc[:, -2]
        map_data.sort_values(by='Confirmed', ascending=False, inplace=True)

        # Moving India to the first row in the datatable
        map_data["new"] = range(1, len(map_data) + 1)
        map_data.loc[map_data[map_data['Country/Region'] == 'India'].index.values, 'new'] = 0
        map_data = map_data.sort_values("new").drop('new', axis=1)
        self.map_data = map_data

if __name__ == '__main__':
    c19 = Cov19()