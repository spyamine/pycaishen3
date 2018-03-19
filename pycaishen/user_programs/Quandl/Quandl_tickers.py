import abc

import pandas as pd
from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.user_programs.user_programs_settings import Configurer

import os
import ntpath
import time as t  # to use the date to name our file
import zipfile
import requests

class IO_Files(object):


    current_directory = os.getcwd()

    # http://www.tutorialspoint.com/python/python_files_io.htm


    def get_files(self,extension=None, mydir=current_directory):
        '''
        this function get the list of all the files having a specific extension
        mydir and extestion are strings
        exemple :
        mydir = "/" # will start from the root of the hard drive
        extension = ".txt" will get the txt files
        if this function is called empty it will return all the files within the current directory where the python code is opened
        we could use get_files() : to get the above
        or get_files(".txt") to get all the txt extension files
        or get_files(".txt","/") to list all the files with txt extension in all the hard drive
        get_files(None, ) : will list all the files in the current directory
        use this : current_directory =  os.getcwd() # to get the current directory
        get_files returns a  dictionary containing the name of the files as key and the path of this files as value
        You can access them quickly files_names = dict.keys() and files_path = dict.values()
        this will return a list of those items
        '''
        print(("Reading files in the folder :' %s ' looking for '%s' as extension " % (mydir, extension)))

        files_list = []
        files_dict = {}

        if extension != None:

            for root, dirs, files in os.walk(mydir):
                for file in files:
                    if extension != None:
                        if file.endswith(extension):
                            files_list.append({self.get_file_name(os.path.join(root, file)): os.path.join(root, file)})
                            files_dict[self.get_file_name(os.path.join(root, file))] = os.path.join(root, file)
                            # files_names.append(p)
        else:
            for root, dirs, files in os.walk(mydir):
                for file in files:
                    files_list.append({self.get_file_name(os.path.join(root, file)): os.path.join(root, file)})
                    files_dict[self.get_file_name(os.path.join(root, file))] = os.path.join(root, file)
                    # files_names.append(path_leaf(os.path.join(root, file)))

        return files_dict

    def get_file_name(self,path):
        """
        this function get the file name from the path a return a function
        """
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    def read_file_to_list(self,filename, separator=""):
        """
        This function will read the document and return list of lists containing in the first line the header and then the values of the filename
        the document
        """

        fo = open(filename)
        print(("opening the file ", "'", fo.name, "'"))
        lines = []
        # reading the file line by line
        for line in fo:
            lines.append(line.split(separator))
        fo.close()
        return lines

    def read_file_to_list_of_dict(self,filename, separator=""):
        """
        This function will read the document and return list of lists containing in the first line the header and then the values of the filename
        the document
        """
        lines = self.read_file_to_list(filename, separator)

        keys = lines[0]
        # print keys
        # print len(lines)
        # print lines[len(lines)-1]
        keys = self.get_keys_from_file(filename, separator)

        content = []
        for i in range(1, len(lines)):
            content.append(dict(list(zip(keys, lines[i]))))
        return content

    def get_keys_from_file(self,filename, separator=None):
        fo = open(filename)
        # print "opening the file " ,"'", fo.name,"'"

        # reading the file line by line
        for line in fo:
            keys = line.split(separator)
            break
        fo.close()
        return keys

    def print_a_list(self,list, max_print=10):
        """
        this function will print a list items until a max number
        """
        i = min(max_print, len(list))
        for i in range(0, len(list)):
            print((i, list[i]))
            if i == max_print:
                break

    def write_a_text(self,message, filename):
        old_message = ""
        try:
            f = open(filename, "r")

            for line in f:
                old_message += line
            f.close()
        except:
            pass

        f = open(filename, "w")
        f.write(str(old_message + "\n" + message))
        f.close()

    def log_error(self,message):
        # get the current time/date
        date = t.localtime(t.time())
        filename = '%d_%d_%d_log_error.log' % (date[1], date[2], (date[0] % 100))
        time_log = '%d:%d:%d : ' % (date[3], date[4], (date[5]))
        self.write_a_text(time_log + message, filename)

    def make_dir(self,dirpath):
        # creating the directory where the action will happen
        try:
            print("Creating the directory to hold the reference sheet")
            os.mkdir(dirpath)
        except OSError:
            pass

    def downloadFile(self,link, filename, dirpath =current_directory):
        print(("downloading reference file from ", link))

        print("downloading with requests")
        r = requests.get(link)
        try:
            if dirpath != self.current_directory:
                self.make_dir(dirpath)
        except:
            pass

        with open(dirpath + "/" + filename, "wb") as code:
            code.write(r.content)

    def unzipFile(self,filename, dirname=None, dirpath=current_directory):
        # unziping the file
        print("unziping the file ")
        if dirname!= None:
            with zipfile.ZipFile(dirname + "/" + filename, "r") as z:
                z.extractall(dirpath)
        else:
            with zipfile.ZipFile(dirpath + "/" + filename, "r") as z:
                z.extractall(dirpath)

    def deleteDir(self,path):
        # deleting the directory

        try:
            import platform
        except:
            pass

        current_system = platform.system()
        folder_separator = "/"
        if current_system == "Windows":
            folder_separator = "\\"

        try:
            print(("Deleting the directory ", path))

            for file in os.listdir(path):
                try:
                    file_path = path + folder_separator + file
                    os.remove(file_path)
                except:
                    pass

            os.rmdir(path)
        except OSError:
            pass


    def excel_to_pandas(self,path,sheetname='Sheet1'):
        try:
            import pandas as pd

            # load the sheet into the Pandas Dataframe structure
            df = pd.read_excel(path, sheetname)

            return df

        except:
            pass

    def pandas_to_excel(self,dataframe,path,sheetname='Sheet1',index = False):
        try:
            import pandas as pd
            from pandas import ExcelWriter
            # load the sheet into the Pandas Dataframe structure
            writer = ExcelWriter(path)
            dataframe.to_excel(writer, sheetname, index)
            writer.save()

        except:
            pass

class QuandlTickers(object):
    """

    class to get the Quandl Tickers
    """



    def get(self, libraries,delete = True):
        """

        :param libraries: list of quandl libraries
        :return: dataframe having columns as [symbol, name,libname] containing the references needed
        we return lib names too because each library returns a different set of dataset
        """
        # checking if the library is a string or a list
        # print type (library)
        if type(libraries) == str:
            libraries=[libraries]
        elif type(libraries) != list:
            print("library type must be a list or a string ")
            return
            # self.logger.warning("library type must be a list or a string ")

        io = IO_Files()
        current_directory = io.current_directory
        folder = Configurer.QUANDL_TICKERS_FOLDER
        folder_path = current_directory + "/" + folder
        for lib in libraries:
            if len(lib.split("/"))>1:
                quandl_link = "https://www.quandl.com/api/v3/databases/"+lib+"codes?api_key="+ Configurer.QUANDL_API_KEY
            else:
                quandl_link = "https://www.quandl.com/api/v3/databases/" + lib + "/codes?api_key=" + Configurer.QUANDL_API_KEY
            file_name = lib+".zip"
            io.downloadFile(quandl_link,file_name,folder_path)
            io.unzipFile(file_name,folder,folder_path)

        files = io.get_files("csv",folder_path)
        path_list = list(files.values())
        path_list_filenames = list(files.keys())

        libraries_name =[]
        # getting the libraries name
        for filename in path_list_filenames:
            libraries_name.append(filename.split("-")[0])


        #reading the csv's and storing them in a unified dataframe

        df_list =[]
        i = 0
        for path in path_list:
            print(path)
            # df.append(pd.read_csv(path))
            intermediary_df =  pd.read_csv(path)
            intermediary_df["Database"] = libraries_name[i]
            intermediary_df.columns = ["Symbol","Name","Database"]

            #remove unnecessary labeling after "," in name column
            intermediary_df["Name"] = intermediary_df["Name"].str.split(",").str.get(0)

            print((libraries_name[i] + " symbols: " + str(intermediary_df.shape[0])))

            # print self ._get_database_name_from_dataframe(intermediary_df)
            df_list.append(intermediary_df)
            i = i+ 1

        # df = df.set_index('Symbol')

        # print "symbols downloaded: " + str(df.shape[0])
        # deleting the folder we used


        if delete:
            io.deleteDir(folder_path)

        return df_list

    def _get_database_name_from_dataframe(self,dataframe):

        return dataframe["Database"].iloc[0]

    def save(self,dataframe_list, library_name=Configurer.QUANDL_SYMBOL_LIBRARY,target_io_name="Arctic"):

        """

        :param dataframe: dataframe containing the data i need to save
        :param library_name: library where the data will be saved depending on the target IO
        :param save_by_libraries: Boolean to save or not save the symbols by libraries, True is much faster
        :param append:
        :param target_io_name:
        :return:
        """
        print(("saving tickers in " + library_name))
        #setting up the IO engine
        engine = PycaishenStorage(target_io_name)

        for dataframe in dataframe_list:
            symbol = self._get_database_name_from_dataframe(dataframe)
            engine.write(symbol,dataframe,library_name)



if __name__ == '__main__':

    lib = Configurer.QUANDL_LIBRARIES_LIST
    print(lib)

    Quandl_ticker = QuandlTickers()
    data = Quandl_ticker.get(lib)
    Quandl_ticker.save(data)




