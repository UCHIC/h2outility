import base64
import hashlib
from collections import defaultdict
import datetime
from multiprocessing import Process, Queue
from time import sleep

from wx.lib.pubsub import pub
import pandas as pd
from pandas import DataFrame

from Common import *
from GAMUTRawData.odmdata import QualityControlLevel, Series, Site, Source, Qualifier, Variable, Method
from GAMUTRawData.odmservices import SeriesService, ServiceManager

this_file = os.path.realpath(__file__)
directory = os.path.dirname(os.path.dirname(this_file))

sys.path.insert(0, directory)

time_format = '%Y-%m-%d'
formatString = '%s  %s: %s'
service_manager = ServiceManager()


class FileDetails(object):
    def __init__(self, site_code="", site_name="", file_path="", file_name="", variable_names=None):
        self.coverage_start = None
        self.coverage_end = None
        self.file_path = file_path
        self.file_name = file_name
        self.site_code = site_code
        self.site_name = site_name
        self.variable_names = [] if variable_names is None else variable_names
        self.is_empty = True
        self.created = False

    def __str__(self):
        fd_str = '{site} - {s_name} - {f_name}'
        return fd_str.format(site=self.site_code, s_name=self.site_name, f_name=self.file_name)


class H2OManagedResource:
    def __init__(self, resource=None, odm_series=None, resource_id='', hs_account_name='', odm_db_name='',
                 single_file=False, chunk_years=False, associated_files=None):
        self.resource_id = resource_id  # type: str
        self.resource = resource  # type: HydroShareResource
        self.selected_series = odm_series if odm_series is not None else {}  # type: dict[int, H2OSeries]
        self.hs_account_name = hs_account_name  # type: str
        self.odm_db_name = odm_db_name  # type: str
        self.single_file = single_file  # type: bool
        self.chunk_years = chunk_years  # type: bool
        self.associated_files = associated_files if associated_files is not None else []  # type: list[str]

    @property
    def public(self):
        if self.resource and hasattr(self.resource, 'public'):
            return getattr(self.resource, 'public')
        return False

    @property
    def subjects(self):
        if self.resource:
            if hasattr(self.resource, 'subjects'):
                return getattr(self.resource, 'subjects')
            elif hasattr(self.resource, 'keywords'):
                return getattr(self.resource, 'keywords')
        return []

    @property
    def keywords(self):
        return self.subjects

    def __dict__(self):
        return {'resource': self.resource, 'selected_series': self.selected_series,
                'hs_account_name': self.hs_account_name, 'resource_id': self.resource_id,
                'single_file': self.single_file, 'chunk_years': self.chunk_years,
                'odm_db_name': self.odm_db_name, 'associated_files': self.associated_files}

    def to_dict(self):
        return self.__dict__()

    def __str__(self):
        if self.resource is not None:
            return 'Managed resource {} with {} series'.format(self.resource.title, len(self.selected_series))
        else:
            return 'Managed resource with ID {} and {} series'.format(self.resource_id, len(self.selected_series))


def _OdmDatabaseConnectionTestTimed(queue):
    db_auth = queue.get(True)
    if service_manager.test_connection(db_auth):
        queue.put(True)
    else:
        queue.put(False)


class OdmDatasetConnection:
    def __init__(self, values=None):
        self.name = ""
        self.engine = ""
        self.user = ""
        self.password = ""
        self.address = ""
        self.database = ""
        self.port = ""

        if values is not None:
            self.name = values['name'] if 'name' in values else ""
            self.engine = values['engine'] if 'engine' in values else ""
            self.user = values['user'] if 'user' in values else ""
            self.password = values['password'] if 'password' in values else ""
            self.address = values['address'] if 'address' in values else ""
            self.database = values['db'] if 'db' in values else ""
            self.port = values['port'] if 'port' in values else ""

    def __str__(self):
        return 'Dataset connection details {}'.format(self.name)

    def VerifyConnection(self):
        queue = Queue()
        result = False
        process = Process(target=_OdmDatabaseConnectionTestTimed, args=(queue,))
        try:
            process.start()
            queue.put(self.ToDict())
            sleep(2)
            result = queue.get(True, 8)
        except Exception as exc:
            print(exc)

        if process.is_alive():
            process.terminate()
            process.join()
        return result

    def ToDict(self):
        return {'engine': self.engine, 'user': self.user, 'password': self.password, 'address': self.address,
                'db': self.database, 'port': self.port}


DELIMITER = '# {}'.format('-' * 90)


def createFile(filepath):
    try:
        print('Creating new file {}'.format(filepath))
        return open(filepath, 'w')
    except Exception as e:
        print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
        return None


def GetTimeSeriesDataframe(series_service, series_list, site_id, qc_id, source_id, methods, variables, starting_date,
                           year=None):
    q_list = []
    censor_list = []

    dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, variables, year,
                                                     starting_date=starting_date,
                                                     chunk_size=APP_SETTINGS.QUERY_CHUNK_SIZE,
                                                     timeout=APP_SETTINGS.DATAVALUES_TIMEOUT)

    if qc_id == 0 or len(variables) != 1 or len(methods) != 1:

        csv_table = pd.pivot_table(dataframe,
                                   index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                   columns=['VariableCode', 'MethodID'],
                                   values='DataValue')

        nodata_values = {}
        for series in series_list:
            nodata_values[(series.variable_code, series.method_id)] = series.variable.no_data_value

        csv_table.fillna(value=nodata_values, inplace=True)

    else:
        method = next(iter(methods))
        variable = next(iter(variables))

        dataframe.fillna(value={'DataValue': series_list[0].variable.no_data_value}, inplace=True)

        q_list = [[q.id, q.code, q.description] for q in
                  series_service.get_qualifiers_by_series_details(site_id, qc_id, source_id, method, variable)]

        # Get the qualifiers that we use in this series, merge it with our DataValue set
        q_df = DataFrame(data=q_list, columns=["QualifierID", "QualifierCode", "QualifierDescription"])

        csv_table = dataframe.merge(q_df, how='left', on="QualifierID")  # type: DataFrame

        csv_table.set_index(["LocalDateTime", "UTCOffset", "DateTimeUTC"], inplace=True)
        for column in csv_table.columns.tolist():

            if column not in ["DataValue", "CensorCode", "QualifierCode"]:
                csv_table.drop(column, axis='columns', inplace=True)

        colmapper = {'DataValue': (series_list[0].variable_code, series_list[0].method_id)}
        csv_table.rename(columns=colmapper, inplace=True)

        if 'CensorCode' in csv_table:
            censor_list = set(csv_table['CensorCode'].tolist())

    del dataframe  # free up some space in memory I guess?

    return csv_table, q_list, censor_list  # don't ask questions... just let it happen


def BuildCsvFile(series_service, series_list, year=None, failed_files=None):  # type: (SeriesService, list[Series], int, list[tuple(str)]) -> str | None

    if failed_files is None:
        failed_files = list()

    try:
        if len(series_list) == 0:
            print('Cannot generate a file for no series')
            return None
        variables = set([series.variable_id for series in series_list if series is not None])
        variable_codes = set([series.variable_code for series in series_list if series is not None])
        methods = set([series.method_id for series in series_list if series is not None])
        qc_ids = set([series.quality_control_level_id for series in series_list if series is not None])
        site_ids = set([series.site_id for series in series_list if series is not None])
        source_ids = set([series.source_id for series in series_list if series is not None])

        if len(qc_ids) == 0 or len(site_ids) == 0 or len(source_ids) == 0:
            print('Series provided are empty or invalid')
        elif len(qc_ids) > 1 or len(site_ids) > 1 or len(source_ids) > 1:
            print('Cannot create a file that contains multiple QC, Site, or Source IDs')
            print('{}: {}'.format(varname(qc_ids), qc_ids))
            print('{}: {}'.format(varname(site_ids), site_ids))
            print('{}: {}\n'.format(varname(source_ids), source_ids))
        elif len(variables) == 0 or len(methods) == 0:
            print('Cannot generate series with no {}'.format(varname(variables if len(variables) == 0 else methods)))
        else:
            try:
                site = series_list[0].site  # type: Site
            except Exception:
                site = Site(site_code=series_list[0].site_code, site_name=series_list[0].site_name)


            source = series_list[0].source  # type: Source
            qc = series_list[0].quality_control_level  # type: QualityControlLevel
            variables = list(variables)
            variable_codes = list(variable_codes)
            methods = list(methods)


            fname_components = [site.code]

            if len(variable_codes) == 1:
                fname_components.append(variable_codes[0])

            # not sure why it was writing the method id in the file name. leaving it here just in case.
            # if len(methods) == 1:
            #     fname_components.append('MethodID_%s' % methods[0])

            fname_components.append('SourceID_%s' % source.id)
            fname_components.append('QC_%s' % qc.code)

            if year is not None:
                fname_components.append('Year_%s' % year)

            file_name = '%s.csv' % '_'.join(fname_components)

            fpath = os.path.join(APP_SETTINGS.DATASET_DIR, file_name)

            """
            This used to check if the file already existed on disk and
            wouldn't upload the file if true. I commented this out
            because it was causing inconsistent behaviour. Leaving it
            here as a reference in case something pops up later.
            """
            # if os.path.exists(fpath):
            #     csv_data = parseCSVData(fpath)
            #     csv_end_datetime = csv_data.localDateTime
            # else:
            #     csv_end_datetime = None
            csv_end_datetime = None


            stopwatch_timer = None
            if APP_SETTINGS.VERBOSE:
                stopwatch_timer = datetime.datetime.now()
                print('Querying values for file {}'.format(fpath))

            dataframe, qualifier_codes, censorcodes = GetTimeSeriesDataframe(series_service, series_list, site.id, qc.id, source.id, methods, variables, csv_end_datetime, year)

            if APP_SETTINGS.VERBOSE:
                print('Query execution took {}'.format(datetime.datetime.now() - stopwatch_timer))

            if dataframe is not None:

                if csv_end_datetime is None:
                    dataframe.sort_index(inplace=True)

                    # `varheaders` and `duplicatevarcounter` are used in `preheader_column_mapper()`
                    # to determine which number to append to duplicate variable codes
                    varheaders = [x[0] if not isinstance(x, str) else x for x in dataframe.columns]
                    duplicatevarcounter = defaultdict(lambda: 0)
                    for col in dataframe.columns:
                        # look for variable codes that appear more than once in the columns
                        # of `dataframe`, and add those to `duplicatevarcounter`
                        try:
                            var, _ = col
                            if varheaders.count(var) > 1:
                                duplicatevarcounter[var] += 1
                        except ValueError:
                            continue

                    def preheader_column_mapper(col):
                        """
                        Used to rename columns of `dataframe`. Columns of `dataframe` are
                        tuples in the form of `(<variable name>, <method ID>)`. Appends a
                        number to duplicate variable codes in a sequential order.

                        i.e., the columns: `[("Temp", 5), ("Temp", 6), ("DO", 9)]`
                        become -> [("Temp-1", 5), ("Temp-2", 6), ("DO", 9)]`

                        :param col: a column of `dataframe`
                        :return: tuple(str, int)
                        """
                        try:
                            var, methid = col
                            if var in duplicatevarcounter:
                                varheaders.pop(varheaders.index(var))
                                dup_count = duplicatevarcounter.get(var) - varheaders.count(var)
                                newvar = '%s-%s' % (var, dup_count)
                                return newvar, methid
                        except ValueError:
                            # If one column name is a tuple, all column names must be tuples
                            return col, None

                        return col

                    # call set_axis to rename duplicate column names
                    dataframe.set_axis('columns', dataframe.columns.map(preheader_column_mapper))

                    # build the headers
                    headers = BuildSeriesFileHeader(series_list, site, source, qualifier_codes, censorcodes, dataframe=dataframe)

                    # call set_axis again to remove multi-level column names and get the expected CSV output
                    dataframe.set_axis('columns', dataframe.columns.map(lambda x: x[0] if len(x) > 1 else x))  #

                    if WriteSeriesToFile(fpath, dataframe, headers):
                        return fpath
                    else:
                        print('Unable to write series to file {}'.format(fpath))
                        failed_files.append((fpath, 'Unable to write series to file'))

                else:
                    if AppendSeriesToFile(fpath, dataframe):
                        return fpath
                    else:
                        print('Unable to append series to file {}'.format(fpath))
                        failed_files.append((fpath, 'Unable to append series to file'))

            elif dataframe is None and csv_end_datetime is not None:
                print('File exists but there are no new data values to write')
                # return fpath
            else:
                print('No data values exist for this dataset')
                failed_files.append((fpath, 'No data values found for file'))
    except TypeError as e:
        print('Exception encountered while building a csv file: {}'.format(e))
    return None


def AppendSeriesToFile(csv_name, dataframe):
    if dataframe is None and not APP_SETTINGS.SKIP_QUERIES:
        print('No dataframe is available to write to file {}'.format(csv_name))
        return False
    elif dataframe is None and APP_SETTINGS.SKIP_QUERIES:
        print('Writing test datasets to file: {}'.format(csv_name))
        return True
    try:
        file_out = open(csv_name, 'a')
        message = 'Writing datasets to file: {}'.format(csv_name)
        print(message)
        pub.sendMessage('logger', message=message)
        dataframe.to_csv(file_out, header=None)
        file_out.close()
    except Exception as e:
        print('---\nIssue encountered while appending to file: \n{}\n{}\n---'.format(e, e.message))
        return False
    return True


def WriteSeriesToFile(csv_name, dataframe, headers):
    if dataframe is None and not APP_SETTINGS.SKIP_QUERIES:
        print('No dataframe is available to write to file {}'.format(csv_name))
        return False
    elif dataframe is None and APP_SETTINGS.SKIP_QUERIES:
        print('Writing test datasets to file: {}'.format(csv_name))

        return True
    file_out = createFile(csv_name)
    if file_out is None:
        print('Unable to create output file {}'.format(csv_name))
        return False
    else:
        # Write data to CSV file
        print('Writing datasets to file: {}'.format(csv_name))
        pub.sendMessage('logger', message='Creating dataset file: %s' % os.path.basename(csv_name))
        file_out.write(headers)
        dataframe.to_csv(file_out)
        file_out.close()
    return True


def GetSeriesYearRange(series_list):
    start_date = None
    end_date = None
    for odm_series in series_list:
        if start_date is None or start_date > odm_series.begin_date_time:
            start_date = odm_series.begin_date_time
        if end_date is None or end_date < odm_series.end_date_time:
            end_date = odm_series.end_date_time
    return range(start_date.year, end_date.year + 1)


def BuildSeriesFileHeader(series_list, site, source, qualifier_codes=None, censorcodes=None, dataframe=None):
    """
    Creates a file header for CSV files

    :param series_list:
    :param site:
    :param source:
    :param qualifier_codes:
    :param censorcodes:
    :return: a tuple containing the header and a list of the column names for a dataframe
    """

    if qualifier_codes is None:
        qualifier_codes = list()

    if censorcodes is None:
        censorcodes = set()

    header = ''

    if len(series_list) == 1:
        var_data = ExpandedVariableData(series_list[0].variable, series_list[0].method)
    else:
        var_data = CompactVariableData()

        mapped = []  # [(column_name, series), ...]
        for column in dataframe.columns:
            colname, methid = column
            for series in series_list:
                if series.method_id == methid:
                    mapped.append((colname, series))
                    continue

        for colname, series in mapped:
            var_data.variable_method_data.append((colname, series.variable, series.method))

    source_info = SourceInfo()
    source_info.setSourceInfo(source.organization, source.description, source.link, source.contact_name, source.phone,
                              source.email, source.citation)
    header += generateSiteInformation(site)
    header += var_data.printToFile() + '#\n'
    header += source_info.outputSourceInfo() + '#\n'
    if len(censorcodes):
        header += generateCensorCodes()
    header += generateQualifierCodes(qualifier_codes) + '#\n'

    return header


def generate_column_names(series_list):  # type: (any) -> [str]
    """
    Looks for duplicate variable codes and appends a number if collisions exist.

    Example:
        codes = clean_variable_codes(['foo', 'bar', 'foo'])
        print(codes)
        # ['foo-1', 'bar', 'foo-2']
    """
    varcodes = [s.variable_code for s in series_list]
    for varcode in varcodes:
        count = varcodes.count(varcode)
        if count > 1:
            counter = 1
            for i in range(0, len(varcodes)):
                if varcodes[i] == varcode:
                    varcodes[i] = '%s-%s' % (varcode, counter)
                    counter += 1
    return varcodes


def generateSiteInformation(site):
    """

    :param site: Site for which to generate the header string
    :type site: Site
    :rtype: str
    """
    file_str = ""
    file_str += "# Site Information\n"
    file_str += "# ----------------------------------\n"
    file_str += "# SiteCode: " + str(site.code) + "\n"
    file_str += "# SiteName: " + str(site.name) + "\n"
    file_str += "# Latitude: " + str(site.latitude) + "\n"
    file_str += "# Longitude: " + str(site.longitude) + "\n"
    file_str += "# LatLonDatum: " + str(site.spatial_ref.srs_name) + "\n"
    file_str += "# Elevation_m: " + str(site.elevation_m) + "\n"
    file_str += "# ElevationDatum: " + str(site.vertical_datum) + "\n"
    file_str += "# State: " + str(site.state) + "\n"
    file_str += "# County: " + str(site.county) + "\n"
    file_str += "# Comments: " + str(site.comments) + "\n"
    file_str += "# SiteType: " + str(site.type) + "\n"
    file_str += "#\n"
    return file_str


def generateCensorCodes():

    return "# Censor Codes\n" + \
           "# ----------------------------------\n" + \
           "# nc: not censored\n" + \
           "#\n"


def generateQualifierCodes(codes):  # type: ([(int, str, str)]) -> str

    if not len(codes):
        return ""

    header = '# Qualifier Codes\n# ----------------------------------\n'

    for code in codes:
        _, abrv, definition = code
        header += '# %s: %s\n' % (abrv, definition)

    return header + '#\n'


def parseCSVData(filePath):
    csvFile = open(filePath, "r")
    lastLine = getLastLine(csvFile)
    csvFile.close()
    return getDateAndNumCols(lastLine)


def getLastLine(targetFile):
    firstCharSeek = ''
    readPosition = -3
    prevLine = result = ""
    while firstCharSeek != '\n':
        targetFile.seek(readPosition, os.SEEK_END)
        readPosition -= 1
        result = prevLine #last line was being deleted. So I saved a temp to keep it
        prevLine = targetFile.readline()
        firstCharSeek = prevLine[0]
    return result


def getDateAndNumCols(lastLine):
    strList = lastLine.split(",")
    dateTime = datetime.datetime.strptime(strList[0], '%Y-%m-%d %H:%M:%S')
    count = 0
    for value in strList:
        isValueCorrect = strList.index(value) > 2 and value != " \n"
        if isValueCorrect:
            count += 1
    return ReturnValue(dateTime, count)


class ReturnValue:
    def __init__(self, dateTime, noOfVars):
        self.localDateTime = dateTime
        self.numCols = noOfVars


class SourceInfo:
    def __init__(self, use_citation=True):
        self.organization = ""
        self.sourceDescription = ""
        self.sourceLink = ""
        self.contactName = ""
        self.phone = ""
        self.email = ""
        self.citation = ""
        self.use_citation = use_citation

    def setSourceInfo(self, org, srcDesc, srcLnk, cntctName, phn, email, citn):
        self.organization = org
        self.sourceDescription = srcDesc
        self.sourceLink = srcLnk
        self.contactName = cntctName
        self.phone = phn
        self.email = email
        self.citation = citn

    def outputSourceInfo(self):
        outputStr = "# Source Information\n# ----------------------------------\n"
        outputStr += self.sourceOutHelper("Organization", self.organization)
        outputStr += self.sourceOutHelper("SourceDescription", self.sourceDescription)
        outputStr += self.sourceOutHelper("SourceLink", self.sourceLink)
        outputStr += self.sourceOutHelper("ContactName", self.contactName)
        outputStr += self.sourceOutHelper("Phone", self.phone)
        outputStr += self.sourceOutHelper("Email", self.email)
        if self.use_citation:
            outputStr += self.sourceOutHelper("Citation", self.citation)
        return outputStr

    def sourceOutHelper(self, title, value):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()

        if isinstance(value, unicode):
            value = value.encode('utf-8').strip()
        return '# {}: {} \n'.format(title, value)


class VariableFormatter(object):
    """
    Abstract class - basically here to make it clear inherited classes
    need to implement the methods in this class.
    """
    def __init__(self):
        pass

    def formatHelper(self, label, value):
        raise NotImplemented

    def printToFile(self):
        raise NotImplemented


class ExpandedVariableData(VariableFormatter):
    def __init__(self, var, method):
        super(ExpandedVariableData, self).__init__()
        self.varCode = var.code
        self.varName = var.name
        self.valueType = var.value_type
        self.dataType = var.data_type
        self.gralCategory = var.general_category
        self.sampleMedium = var.sample_medium
        self.varUnitsName = var.variable_unit.name
        self.varUnitsType = var.variable_unit.type
        self.varUnitsAbbr = var.variable_unit.abbreviation
        self.noDV = int(var.no_data_value) if var.no_data_value.is_integer() else var.no_data_value
        self.timeSupport = var.time_support
        self.timeSupportUnitsAbbr = var.time_unit.abbreviation
        self.timeSupportUnitsName = var.time_unit.name
        self.timeSupportUnitsType = var.time_unit.type
        self.methodDescription = method.description
        self.methodLink = method.link if method.link is not None else "None"
        if not self.methodLink[-1:].isalnum():
            self.methodLink = self.methodLink[:-1]

    def printToFile(self):
        formatted = ""
        formatted += "# Variable and Method Information\n"
        formatted += "# ----------------------------------\n"
        formatted += self.formatHelper("VariableCode", self.varCode)
        formatted += self.formatHelper("VariableName", self.varName)
        formatted += self.formatHelper("ValueType", self.valueType)
        formatted += self.formatHelper("DataType", self.dataType)
        formatted += self.formatHelper("GeneralCategory", self.gralCategory)
        formatted += self.formatHelper("SampleMedium", self.sampleMedium)
        formatted += self.formatHelper("VariableUnitsName", self.varUnitsName)
        formatted += self.formatHelper("VariableUnitsType", self.varUnitsType)
        formatted += self.formatHelper("VariableUnitsAbbreviation", self.varUnitsAbbr)
        formatted += self.formatHelper("NoDataValue", self.noDV)
        formatted += self.formatHelper("TimeSupport", self.timeSupport)
        formatted += self.formatHelper("TimeSupportUnitsAbbreviation", self.timeSupportUnitsAbbr)
        formatted += self.formatHelper("TimeSupportUnitsType", self.timeSupportUnitsType)
        formatted += self.formatHelper("TimeSupportUnitsName", self.timeSupportUnitsName)
        formatted += self.formatHelper("MethodDescription", self.methodDescription)
        formatted += self.formatHelper("MethodLink", self.methodLink)
        return formatted

    def formatHelper(self, title, var):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()
        if isinstance(var, unicode):
            var = var.encode('utf-8').strip()

            if ',' in var:
                return '"# {}: {}"\n'.format(title, var)

        return '# {}: {} \n'.format(title, var)


class CompactVariableData(VariableFormatter):
    def __init__(self):
        super(CompactVariableData, self).__init__()
        self.variable_method_data = []


    def printToFile(self):

        header = "# Variable and Method Information\n"
        header += "# ----------------------------------\n"

        rows = []

        for column_name, variable, method in self.variable_method_data:

            definitions = []

            if method.link is None:
                tempVarMethodLink = "None"
            else:
                tempVarMethodLink = method.link if method.link[-1:].isalnum() else method.link[-1:]

            definitions.append(self.formatHelper("Column", column_name))
            definitions.append(self.formatHelper("VariableCode", variable.code))
            definitions.append(self.formatHelper("VariableName", variable.name))
            definitions.append(self.formatHelper("MethodID", method.id))
            definitions.append(self.formatHelper("ValueType", variable.value_type))
            definitions.append(self.formatHelper("DataType", variable.data_type))
            definitions.append(self.formatHelper("GeneralCategory", variable.general_category))
            definitions.append(self.formatHelper("SampleMedium", variable.sample_medium))
            definitions.append(self.formatHelper("VariableUnitsName", variable.variable_unit.name))
            definitions.append(self.formatHelper("VariableUnitsType", variable.variable_unit.type))
            definitions.append(self.formatHelper("VariableUnitsAbbreviation", variable.variable_unit.abbreviation))
            definitions.append(self.formatHelper("NoDataValue", variable.no_data_value))
            definitions.append(self.formatHelper("TimeSupport", variable.time_support))
            definitions.append(self.formatHelper("TimeSupportUnitsAbbreviation", variable.time_unit.abbreviation))
            definitions.append(self.formatHelper("TimeSupportUnitsName", variable.time_unit.name))
            definitions.append(self.formatHelper("TimeSupportUnitsType", variable.time_unit.type))
            definitions.append(self.formatHelper("MethodDescription", method.description))
            definitions.append(self.formatHelper("MethodLink", tempVarMethodLink)[:-2])

            rows.append(definitions)

        definitions = "\n".join(['"# %s"' % ' | '.join(row) for row in rows])

        return '%s%s\n' % (header, definitions)

    def formatHelper(self, title, var):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()
        if isinstance(var, unicode):
            var = var.encode('utf-8').strip()
        return '{0}: {1}'.format(title, var)
