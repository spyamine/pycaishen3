from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData

from datetime import timedelta

import datetime

from pycaishen.user_programs.user_programs_settings import Configurer


"""
request.Append("fields", "DVD_HIST_ALL")
Dim ovr As Element = requestOverrides.AppendElement()
        ovr.SetElement(FIELD_ID, "DVD_START_DT")
        ovr.SetElement("value", "20101001")

        ovr.SetElement(FIELD_ID, "DVD_END_DT")
        ovr.SetElement("value", "20151001")
"""
class BloomIndexComposition(object):

    def get_composition(self, index, date = None, remove_tickers_name_from_dataframe = False):

        if type(index)== type("str"):
            index = [index]

        datasource = "bloomberg"
        datasource_tickers = index
        datasource_fields = ["INDX_MWEIGHT_HIST"]
        category = "reference"

        data = PycaishenData()

        if date != None:
            date = self._date_parser(date)

        if date != None:
            options_type = ['override']
            options_fields = ["END_DT"]
            options_values = [date.strftime('%Y%m%d')]
            data.set_datasource_options(datasource, options_type=options_type, options_fields=options_fields,
                                        options_values=options_values)


        data.set_request(datasource_name=datasource,
                          data_source_fields=datasource_fields,
                          data_source_tickers=datasource_tickers,
                          timeseries_type=False,
                           category=category)



        print(("* getting data for %d indexes ....." % (len(index))))

        data_frame_list =  data.fetch_request()
        data_frame_list_final = []
        if date != None:


            for dataframe in data_frame_list:
                date_column_name = self._get_symbol_from_dataframe(dataframe) + ".Date"
                dataframe[date_column_name] = self._date_parser(date)
                # dataframe["index"] = self._get_symbol_from_dataframe(dataframe)


                data_frame_list_final.append(dataframe)

            if remove_tickers_name_from_dataframe:
                return self._remove_tickers_name_from_dataframe_list(data_frame_list_final)
            else:
                return data_frame_list_final
        else:
            if remove_tickers_name_from_dataframe:
                return self._remove_tickers_name_from_dataframe_list(data_frame_list)
            else:
                return data_frame_list

    def _date_parser(self, date):
        if isinstance(date, str):

            date1 = datetime.datetime.utcnow()

            if date is 'midnight':
                date1 = datetime.datetime(date1.year, date1.month, date1.day, 0, 0, 0)
            elif date is 'decade':
                date1 = date1 - timedelta(days=360 * 10)
            elif date is 'year':
                date1 = date1 - timedelta(days=360)
            elif date is 'month':
                date1 = date1 - timedelta(days=30)
            elif date is 'week':
                date1 = date1 - timedelta(days=7)
            elif date is 'day':
                date1 = date1 - timedelta(days=1)
            elif date is 'hour':
                date1 = date1 - timedelta(hours=1)
            # elif type(date) == "str" :
            #     date1 = datetime.datetime.fromstring
            else:
                # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%b %d %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                # format expected '1 Jun 2005 01:33', '%d %b %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%d %b %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.date.strptime(date, '%b %d %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%d %m %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%d %m %y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%Y %m %d')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%Y %m %d')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0
        else:
            date1 = date

        return date1

    def _remove_tickers_name_from_dataframe_list(self,data_frame_list):

        # removing tickers name from the headers of each dataframe to prepare for a merger
        for data_frame in data_frame_list:
            original_columns = data_frame.columns
            # remove tickers from the columns
            columns = [x.split(".")[1] for x in original_columns]
            # build a header as a dictionary
            header = dict(list(zip(original_columns, columns)))
            # rename the dataframe
            data_frame.rename(columns=header, inplace=True)

        return data_frame_list

    def _remove_tickers_name_from_dataframe(self,data_frame):

        # removing tickers name from the headers of each dataframe to prepare for a merger

        original_columns = data_frame.columns
        # remove tickers from the columns
        columns = [x.split(".")[1] for x in original_columns]
        # build a header as a dictionary
        header = dict(list(zip(original_columns, columns)))
        # rename the dataframe
        data_frame.rename(columns=header, inplace=True)

        return data_frame

    def _get_symbol_from_dataframe(self,dataframe):
        symbol = dataframe.columns[0].split('.')[0]
        return symbol

    def storeIndexComposition(self,dataframe_list,library = Configurer.LIB_INDEX_COMPOSITION):

        print("* Storing data in the database")
        storage = PycaishenStorage("arctic")
        i = 0
        for composition in dataframe_list:
            symbol =  self._get_symbol_from_dataframe(composition)
            composition = self._remove_tickers_name_from_dataframe(composition)
            composition["Symbol"] = composition["Index Member"] + " Equity"
            storage.write(symbol,composition,library,append_data=True)
            i = i + 1

        print(("=>  %d / %d index composition data stored successfully " % (i, len(dataframe_list))))

    def get_composition_date(self,index, start_date,end_date,delta_days = 90):
        start_date = self._date_parser(start_date)
        end_date = self._date_parser(end_date)

        if start_date> end_date:
            intermediary = start_date
            start_date = end_date
            end_date = intermediary

        date = start_date
        compositions = []
        while date <= end_date:
            print(("Working on: " + str(date)))
            compositions = compositions + self.get_composition(index,date)
            if date == end_date:
                break
            else:
                date = date +timedelta(delta_days)
                if date > end_date:
                    date = end_date

        return compositions


if __name__ == '__main__':

    import datetime as d

    indexes = Configurer.AFRICAN_INDEXES



    IndexComposition = BloomIndexComposition()
    import datetime as d
    start_date = d.datetime(2012, 1, 1, 0, 0, 0, 0)
    end_date = d.datetime(2017, 1, 23, 0, 0, 0, 0)


    data = IndexComposition.get_composition_date(indexes,start_date,end_date)
    IndexComposition.storeIndexComposition(data)

    # storage = PycaishenStorage("arctic")
    # storage.delete_library("Bloomberg.Index.Composition")
    # indexes =  storage.list_symbols("Bloomberg.Index.Composition")
    # print indexes
    #
    # for index in indexes:
    #     print index
    #     print storage.read(index,"Bloomberg.Index.Composition").head()
    #
